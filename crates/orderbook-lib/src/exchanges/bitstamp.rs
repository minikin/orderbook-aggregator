use serde::{Deserialize, de::IgnoredAny};

use super::{ExchangeConnector, ExchangeError};
use crate::types::OrderBook;

pub struct BitstampConnector;

impl ExchangeConnector for BitstampConnector {
    const NAME: &'static str = "bitstamp";

    #[allow(unused_variables)]
    fn ws_url(&self, pair: &str) -> String {
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
                    bids: super::LevelParser::parse("bitstamp", book.bids),
                    asks: super::LevelParser::parse("bitstamp", book.asks),
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
            "bts:error" => Err(ExchangeError::Format(
                "bts:error received — check pair name and subscription".to_string(),
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
