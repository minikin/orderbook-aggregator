use serde::{Deserialize, de::IgnoredAny};

use super::{ExchangeConnector, ExchangeError};
use crate::types::OrderBook;
pub struct BinanceConnector;

impl ExchangeConnector for BinanceConnector {
    const NAME: &'static str = "Binance";

    fn ws_url(&self, pair: &str) -> String {
        format!(
            "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
            pair.to_lowercase()
        )
    }

    fn subscribe_message(&self, _pair: &str) -> Option<String> {
        // Binance streams data immediately after connecting, so no subscription message is needed.
        None
    }

    fn parse_message(&self, raw: &str) -> Result<Option<OrderBook>, ExchangeError> {
        match from_str::<BinanceMessage<'_>>(raw)? {
            BinanceMessage::Depth(msg) => Ok(Some(msg.into_order_book())),
            // Binance sends {"code":-1121,"msg":"Invalid symbol."} for bad pairs.
            // Treat this as misconfiguration for the configured pair.
            BinanceMessage::Error { code, msg } => Err(ExchangeError::Format(format!(
                "Binance API error {code}: {msg}"
            ))),
            // Connection confirmation {"result":null,"id":1} and similar.
            BinanceMessage::Other(_) => Ok(None),
        }
    }
}

/// Top-level Binance message envelope.
///
/// The `'a` lifetime lets `DepthMessage` borrow price/qty strings directly
/// from the raw JSON input rather than allocating owned `String`s.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum BinanceMessage<'a> {
    /// Depth20 order book snapshot — the normal streaming message.
    // `#[serde(borrow)]` propagates `'de: 'a` from the inner struct to the enum.
    Depth(#[serde(borrow)] DepthMessage<'a>),

    /// API error (e.g. invalid symbol, rate limit).
    Error { code: i32, msg: String },

    /// Any other message (connection confirmation, etc.) — safe to ignore.
    Other(IgnoredAny),
}

/// Binance depth20 snapshot format.
///
/// `&'a str` slices borrow directly from the raw WebSocket frame — no `String`
/// allocation is needed since the values are immediately parsed into `Decimal` by
/// `parse_levels`. The lifetime is confined to `parse_message`'s stack frame.
#[derive(Deserialize, Debug)]
struct DepthMessage<'a> {
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,

    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

impl DepthMessage<'_> {
    fn into_order_book(self) -> OrderBook {
        OrderBook {
            bids: LevelParser::parser("binance", self.bids),
            asks: LevelParser::parser("binance", self.asks),
        }
    }
}
