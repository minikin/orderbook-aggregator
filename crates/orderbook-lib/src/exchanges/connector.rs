use crate::types::OrderBook;

use super::ExchangeError;

/// Implemented by each exchange-specific connector.
pub trait ExchangeConnector: Send + Sync + 'static {
    /// Human-readable exchange name.
    const NAME: &'static str;

    /// Returns the WebSocket URL for the given trading pair (e.g. `"ethbtc"`).
    fn ws_url(&self, pair: &str) -> String;

    /// Returns a JSON subscription message to send immediately after connecting,
    /// or `None` if the exchange starts streaming without a subscription step.
    fn subscribe_message(&self, pair: &str) -> Option<String>;

    /// Parses a raw WebSocket text frame into an [`OrderBook`] snapshot.
    fn parse_message(&self, message: &str) -> Result<Option<OrderBook>, ExchangeError>;
}
