use serde::{Deserialize, de::IgnoredAny};

use crate::exchange_infra::{ExchangeConnector, ExchangeError, LevelParser};
use orderbook_lib::types::OrderBook;

pub struct BitstampConnector;

impl ExchangeConnector for BitstampConnector {
    const NAME: &'static str = "bitstamp";

    fn ws_url(&self, _pair: &str) -> String {
        "wss://ws.bitstamp.net".to_owned()
    }

    fn subscribe_message(&self, pair: &str) -> Option<String> {
        let channel = format!("order_book_{}", pair.to_lowercase());
        Some(format!(
            r#"{{"event":"bts:subscribe","data":{{"channel":"{channel}"}}}}"#
        ))
    }

    fn parse_message(&self, raw: &str) -> Result<Option<OrderBook>, ExchangeError> {
        let msg: WsMessage<'_> = serde_json::from_str(raw)?;

        match msg.event {
            "data" => {
                // For genuine data events the payload is always a BookData object.
                // If somehow it is not (shouldn't happen), treat as ignorable.
                let DataField::Book(book) = msg.data else {
                    return Ok(None);
                };
                let ob = OrderBook {
                    bids: LevelParser::parse("bitstamp", book.bids),
                    asks: LevelParser::parse("bitstamp", book.asks),
                };
                // Guard against malformed messages that parse successfully
                // but contain no levels — an empty snapshot would silently
                // replace a valid book in the aggregator.
                if ob.bids.is_empty() && ob.asks.is_empty() {
                    return Ok(None);
                }
                Ok(Some(ob))
            }
            // A bts:error means our subscription failed (e.g. bad pair name).
            // Treat this as permanent misconfiguration for the configured pair.
            "bts:error" => Err(ExchangeError::InvalidConfig(
                "bts:error received — check pair name and subscription".to_owned(),
            )),
            // Heartbeats, subscription confirmations, bts:request_reconnect, etc.
            _ => Ok(None),
        }
    }
}

/// Both `event` and the level strings borrow directly from the raw frame;
/// no `String` allocation is needed within `parse_message`.
#[derive(Deserialize)]
struct WsMessage<'a> {
    #[serde(borrow)]
    event: &'a str,
    #[serde(borrow)]
    data: DataField<'a>,
}

/// `data` can be either a book-data object (normal events) or any other JSON
/// value. `bts:request_reconnect` sends `"data": ""` (a JSON string, not an
/// object). `#[serde(untagged)]` tries `Book` first; if that fails it falls
/// back to `Other(IgnoredAny)`, so the frame deserializes cleanly instead of
/// producing a parse error that would flood the warning log.
#[derive(Deserialize)]
#[serde(untagged)]
enum DataField<'a> {
    Book(#[serde(borrow)] BookData<'a>),
    Other(IgnoredAny),
}

#[derive(Deserialize)]
struct BookData<'a> {
    #[serde(borrow, default)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow, default)]
    asks: Vec<[&'a str; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    const DATA_EVENT: &str = r#"{
        "event": "data",
        "channel": "order_book_ethbtc",
        "data": {
            "timestamp": "1700000000",
            "microtimestamp": "1700000000000000",
            "bids": [["0.06150000", "12.50000000"], ["0.06100000", "7.00000000"]],
            "asks": [["0.06200000", "9.00000000"], ["0.06250000", "4.00000000"]]
        }
    }"#;

    const HEARTBEAT: &str = r#"{"event":"bts:heartbeat","data":{}}"#;
    const SUBSCRIBED: &str = r#"{"event":"bts:subscription_succeeded","data":{}}"#;
    const REQUEST_RECONNECT: &str = r#"{"event":"bts:request_reconnect","channel":"","data":""}"#;

    #[test]
    fn parses_data_event() {
        let connector = BitstampConnector;
        let book = connector.parse_message(DATA_EVENT).unwrap().unwrap();
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[0].price, dec("0.0615"));
        assert_eq!(book.bids[0].exchange, "bitstamp");
    }

    #[test]
    fn ignores_heartbeat() {
        let connector = BitstampConnector;
        assert!(connector.parse_message(HEARTBEAT).unwrap().is_none());
    }

    #[test]
    fn ignores_subscription_confirmation() {
        let connector = BitstampConnector;
        assert!(connector.parse_message(SUBSCRIBED).unwrap().is_none());
    }

    #[test]
    fn ignores_request_reconnect_with_string_data() {
        // bts:request_reconnect sends `"data": ""` (a JSON string, not an object).
        // This must not produce a parse error or a WARN log entry.
        let connector = BitstampConnector;
        assert!(connector.parse_message(REQUEST_RECONNECT).unwrap().is_none());
    }

    #[test]
    fn bts_error_returns_err() {
        let connector = BitstampConnector;
        let msg = r#"{"event":"bts:error","data":{"code":4,"message":"Bad request"}}"#;
        assert!(connector.parse_message(msg).is_err());
    }

    #[test]
    fn subscribe_message_contains_pair() {
        let connector = BitstampConnector;
        let msg = connector.subscribe_message("ETHBTC").unwrap();
        assert!(msg.contains("order_book_ethbtc"));
    }
}
