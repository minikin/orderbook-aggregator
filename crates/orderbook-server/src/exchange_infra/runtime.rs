use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use orderbook_lib::types::OrderBook;

use super::{ConnectorRuntimeConfig, ExchangeConnector, ExchangeError};

#[derive(Debug, Clone, Copy)]
struct ParseErrorStreak {
    current: u32,
    max: u32,
}

impl ParseErrorStreak {
    fn new(max: u32) -> Self {
        Self {
            current: 0,
            max: max.max(1),
        }
    }

    fn reset(&mut self) {
        self.current = 0;
    }

    fn record(&mut self) -> bool {
        self.current = self.current.saturating_add(1);
        self.current >= self.max
    }

    fn current(&self) -> u32 {
        self.current
    }

    fn max(&self) -> u32 {
        self.max
    }
}

/// Connector runtime state for one exchange stream.
pub struct ConnectorRuntime<C: ExchangeConnector> {
    connector: C,
    pair: String,
    tx: watch::Sender<Option<OrderBook>>,
    config: ConnectorRuntimeConfig,
    backoff: Duration,
    permanent_failures: u32,
}

impl<C: ExchangeConnector> ConnectorRuntime<C> {
    pub fn new(connector: C, pair: String, tx: watch::Sender<Option<OrderBook>>) -> Self {
        Self::with_config(connector, pair, tx, ConnectorRuntimeConfig::default())
    }

    pub(crate) fn with_config(
        connector: C,
        pair: String,
        tx: watch::Sender<Option<OrderBook>>,
        config: ConnectorRuntimeConfig,
    ) -> Self {
        Self {
            connector,
            pair,
            tx,
            backoff: config.initial_backoff,
            config,
            permanent_failures: 0,
        }
    }

    /// Runs a reconnect loop with exponential backoff and jitter.
    ///
    /// The channel type is `Option<OrderBook>` so the aggregator can
    /// distinguish "no data yet" (`None`) from "received at least one real
    /// snapshot" (`Some`).
    ///
    /// Returns `Err` after repeated permanent configuration failures (for
    /// example, an invalid trading pair rejected by the exchange), so the
    /// caller can fail fast instead of retrying forever.
    pub async fn run(mut self) -> Result<(), ExchangeError> {
        loop {
            // Invalidate the last known snapshot before each (re)connect
            // attempt. This prevents the aggregator from combining fresh data
            // from one exchange with stale data from another while a connector
            // is down.
            let _ = self.tx.send(None);

            let session_start = tokio::time::Instant::now();
            let result = self.connect_and_stream().await;

            if matches!(result, Err(ExchangeError::InvalidConfig(_))) {
                self.permanent_failures += 1;
            } else {
                self.permanent_failures = 0;
            }

            if self.permanent_failures >= self.config.max_consecutive_permanent_errors {
                let Err(e) = result else { unreachable!() };
                error!(
                    "{}: stopping after {} consecutive permanent errors: {e}",
                    C::NAME,
                    self.permanent_failures
                );
                return Err(e);
            }

            // Reset backoff if the session was healthy for long enough;
            // double it otherwise to back off from persistent failures.
            self.backoff = if session_start.elapsed() >= self.config.healthy_threshold {
                self.config.initial_backoff
            } else {
                (self.backoff * 2).min(self.config.max_backoff)
            };

            let sleep_ms = reconnect_sleep_ms(self.backoff);

            match result {
                Ok(()) => info!("{}: stream ended, reconnecting in {sleep_ms}ms", C::NAME),
                Err(e) => warn!("{}: {e}, reconnecting in {sleep_ms}ms", C::NAME),
            }

            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    async fn connect_and_stream(&self) -> Result<(), ExchangeError> {
        let url = self.connector.ws_url(&self.pair);
        info!("{}: connecting to {url}", C::NAME);

        let (mut ws, _) = tokio::time::timeout(self.config.connect_timeout, connect_async(&url))
            .await
            .map_err(|_| ExchangeError::Format(format!("{}: connection timed out", C::NAME)))??;

        info!("{}: connected", C::NAME);

        if let Some(msg) = self.connector.subscribe_message(&self.pair) {
            ws.send(Message::Text(msg.into())).await?;
        }

        let mut parse_error_streak = ParseErrorStreak::new(self.config.max_consecutive_parse_errors);

        loop {
            let msg_opt = tokio::time::timeout(self.config.read_timeout, ws.next())
                .await
                .map_err(|_| ExchangeError::Format(format!("{}: read timed out", C::NAME)))?;

            let msg = match msg_opt {
                Some(msg) => msg?,
                None => break, // stream closed cleanly
            };

            let text = match msg {
                Message::Text(t) => t,
                Message::Ping(data) => {
                    ws.send(Message::Pong(data)).await?;
                    continue;
                }
                Message::Close(_) => break,
                _ => continue,
            };

            match self.connector.parse_message(&text) {
                Ok(Some(book)) => {
                    let _ = self.tx.send(Some(book));
                    parse_error_streak.reset();
                }
                Ok(None) => parse_error_streak.reset(),
                Err(e @ ExchangeError::InvalidConfig(_)) => return Err(e),
                // Format-level message errors terminate this session so the
                // outer reconnect loop can apply backoff and establish a clean
                // connection.
                Err(e @ ExchangeError::Format(_)) => return Err(e),
                Err(e) => {
                    warn!(
                        "{}: parse error ({}/{}): {e}",
                        C::NAME,
                        parse_error_streak.current().saturating_add(1),
                        parse_error_streak.max()
                    );
                    if parse_error_streak.record() {
                        return Err(ExchangeError::Format(format!(
                            "{}: reconnecting after {} consecutive parse errors (last error: {e})",
                            C::NAME,
                            parse_error_streak.max()
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

fn reconnect_sleep_ms(backoff: Duration) -> u64 {
    // Equal jitter: sleep for [backoff/2, backoff].
    // Modulo bias is negligible for backoff jitter purposes.
    let half_ms = backoff.as_millis() as u64 / 2;
    let jitter_ms = rand::random::<u64>() % (half_ms + 1);
    half_ms + jitter_ms
}

#[cfg(test)]
mod tests {
    use super::ParseErrorStreak;

    #[test]
    fn parse_error_streak_triggers_at_threshold() {
        let mut streak = ParseErrorStreak::new(3);
        assert!(!streak.record());
        assert!(!streak.record());
        assert!(streak.record());
    }

    #[test]
    fn parse_error_streak_resets_after_success() {
        let mut streak = ParseErrorStreak::new(2);
        assert!(!streak.record());
        streak.reset();
        assert!(!streak.record());
        assert!(streak.record());
    }

    #[test]
    fn parse_error_streak_treats_zero_threshold_as_one() {
        let mut streak = ParseErrorStreak::new(0);
        assert!(streak.record());
        assert_eq!(streak.max(), 1);
    }
}
