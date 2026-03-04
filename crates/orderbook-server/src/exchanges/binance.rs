use serde::{Deserialize, de::IgnoredAny};

use crate::exchange_infra::{ExchangeConnector, ExchangeError, LevelParser};
use orderbook_lib::types::OrderBook;

pub struct BinanceConnector;

impl ExchangeConnector for BinanceConnector {
    const NAME: &'static str = "binance";

    fn ws_url(&self, pair: &str) -> String {
        format!("wss://stream.binance.com:9443/ws/{}@depth20@100ms", pair.to_lowercase())
    }

    fn subscribe_message(&self, _pair: &str) -> Option<String> {
        None // Binance streams data immediately after connecting
    }

    fn parse_message(&self, raw: &str) -> Result<Option<OrderBook>, ExchangeError> {
        match serde_json::from_str::<BinanceMessage<'_>>(raw)? {
            BinanceMessage::Depth(msg) => Ok(Some(msg.into_order_book())),
            // Binance sends {"code":-1121,"msg":"Invalid symbol."} for bad pairs.
            // Treat this as permanent misconfiguration for the configured pair.
            BinanceMessage::Error { code, msg } => {
                Err(ExchangeError::InvalidConfig(format!("Binance API error {code}: {msg}")))
            }
            // Connection confirmation {"result":null,"id":1} and similar.
            BinanceMessage::Other(_) => Ok(None),
        }
    }
}

/// Top-level Binance message envelope.
///
/// The `'a` lifetime lets `DepthMessage` borrow price/qty strings directly
/// from the raw JSON input rather than allocating owned `String`s.
#[derive(Deserialize)]
#[serde(untagged)]
enum BinanceMessage<'a> {
    /// Depth20 order book snapshot — the normal streaming message.
    // `#[serde(borrow)]` propagates `'de: 'a` from the inner struct to the enum.
    Depth(#[serde(borrow)] DepthMessage<'a>),
    /// API error (e.g. invalid symbol, rate limit).
    Error { code: i64, msg: String },
    /// Any other message (connection confirmation, etc.) — safe to ignore.
    Other(IgnoredAny),
}

/// Binance depth20 snapshot format.
///
/// `&'a str` slices borrow directly from the raw WebSocket frame — no `String`
/// allocation is needed since the values are immediately parsed into `Decimal`
/// by `LevelParser`. The lifetime is confined to `parse_message`'s stack frame.
#[derive(Deserialize)]
struct DepthMessage<'a> {
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

impl DepthMessage<'_> {
    fn into_order_book(self) -> OrderBook {
        OrderBook {
            bids: LevelParser::parse("binance", self.bids),
            asks: LevelParser::parse("binance", self.asks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    const SAMPLE: &str = r#"{
        "lastUpdateId": 123456,
        "bids": [["0.06200000", "10.00000000"], ["0.06100000", "5.00000000"]],
        "asks": [["0.06300000", "8.00000000"], ["0.06400000", "3.00000000"]]
    }"#;

    #[test]
    fn parses_depth_snapshot() {
        let connector = BinanceConnector;
        let book = connector.parse_message(SAMPLE).unwrap().unwrap();
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[0].price, dec("0.062"));
        assert_eq!(book.bids[0].exchange, "binance");
        assert_eq!(book.asks[0].price, dec("0.063"));
    }

    #[test]
    fn ws_url_is_lowercased() {
        let connector = BinanceConnector;
        let url = connector.ws_url("ETHBTC");
        assert!(url.contains("ethbtc@depth20@100ms"));
    }

    #[test]
    fn no_subscribe_message() {
        assert!(BinanceConnector.subscribe_message("ethbtc").is_none());
    }
}
