use rust_decimal::Decimal;

use crate::types::{Level, Levels};

/// Converts raw exchange level tuples into validated domain levels.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LevelParser;

impl LevelParser {
    fn parse_positive_decimal(raw: &str) -> Option<Decimal> {
        raw.parse::<Decimal>().ok().filter(|v| *v > Decimal::ZERO)
    }

    /// Parses raw `["price", "amount"]` string tuples into [`Level`]s.
    ///
    /// Generic over `S: AsRef<str>` so callers can pass either owned `String`s
    /// or borrowed `&str` slices (e.g. zero-copy from a raw WebSocket frame).
    ///
    /// Entries where price or amount fails decimal parsing, or where either
    /// value is non-positive, are silently dropped — this guards against
    /// malformed or adversarial exchange messages corrupting the sort order.
    pub(crate) fn parse<S, I>(exchange: &'static str, raw: I) -> Levels
    where
        S: AsRef<str>,
        I: IntoIterator<Item = [S; 2]>,
    {
        let iter = raw.into_iter();
        let (lower_bound, _) = iter.size_hint();
        let mut levels = Levels::with_capacity(lower_bound);

        for [price, amount] in iter {
            let Some(price) = Self::parse_positive_decimal(price.as_ref()) else {
                continue;
            };

            let Some(amount) = Self::parse_positive_decimal(amount.as_ref()) else {
                continue;
            };

            levels.push(Level {
                exchange,
                price,
                amount,
            });
        }

        levels
    }
}
