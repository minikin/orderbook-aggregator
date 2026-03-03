use std::cmp::Ordering;

use rust_decimal::Decimal;

use crate::types::{Level, Levels, OrderBook, Summary};

/// Stateful merger configuration for constructing merged summaries.
#[derive(Debug, Clone, Copy)]
pub struct OrderBookAggregator {
    max_levels: usize,
}

impl Default for OrderBookAggregator {
    fn default() -> Self {
        Self { max_levels: 10 }
    }
}

impl OrderBookAggregator {
    pub const fn new(max_levels: usize) -> Self {
        Self { max_levels }
    }

    /// Merges any number of exchange order books into a single [`Summary`].
    ///
    /// Sorting rules (two-key, as specified in the challenge):
    /// - **Bids**: descending price; ties broken by descending amount
    ///   (largest quantity at the same price ranks first — best for a seller).
    /// - **Asks**: ascending price; ties broken by descending amount
    ///   (largest quantity at the same price ranks first — best for a buyer).
    ///
    /// Retains up to `self.max_levels` on each side.
    /// The spread is `best_ask - best_bid`; zero if either side is empty.
    pub fn merge<'a>(&self, books: impl IntoIterator<Item = &'a OrderBook>) -> Summary {
        if self.max_levels == 0 {
            return Summary {
                spread: Decimal::ZERO,
                bids: Levels::new(),
                asks: Levels::new(),
            };
        }

        let mut bids = Levels::new();
        let mut asks = Levels::new();
        let iter = books.into_iter();
        let (lower_books, _) = iter.size_hint();
        let reserve_hint = lower_books.saturating_mul(self.max_levels);

        bids.reserve(reserve_hint);
        asks.reserve(reserve_hint);

        for book in iter {
            bids.extend(book.bids.iter().cloned());
            asks.extend(book.asks.iter().cloned());
        }

        keep_top_n(&mut bids, self.max_levels, bids_cmp);
        keep_top_n(&mut asks, self.max_levels, asks_cmp);

        // Spread can be negative when the merged book is crossed (best ask from
        // one exchange is below best bid from another). This is intentional —
        // a negative spread signals an arbitrage opportunity and is meaningful
        // data for the consumer. We do not clamp to zero.
        let spread = match (asks.first(), bids.first()) {
            (Some(a), Some(b)) => a.price - b.price,
            _ => Decimal::ZERO,
        };

        Summary { spread, bids, asks }
    }
}

fn bids_cmp(a: &Level, b: &Level) -> Ordering {
    b.price.cmp(&a.price).then_with(|| b.amount.cmp(&a.amount))
}

fn asks_cmp(a: &Level, b: &Level) -> Ordering {
    a.price.cmp(&b.price).then_with(|| b.amount.cmp(&a.amount))
}

fn keep_top_n(levels: &mut Levels, n: usize, cmp: fn(&Level, &Level) -> Ordering) {
    if levels.len() > n {
        levels.select_nth_unstable_by(n, cmp);
        levels.truncate(n);
    }
    levels.sort_unstable_by(cmp);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Level;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn level(exchange: &'static str, price: &str, amount: &str) -> Level {
        Level {
            exchange,
            price: dec(price),
            amount: dec(amount),
        }
    }

    fn book(bids: Vec<Level>, asks: Vec<Level>) -> OrderBook {
        OrderBook {
            bids: bids.into(),
            asks: asks.into(),
        }
    }

    #[test]
    fn bids_sorted_descending() {
        let aggregator = OrderBookAggregator::default();
        let a = book(
            vec![level("binance", "100", "1"), level("binance", "102", "1")],
            vec![level("binance", "103", "1")],
        );
        let b = book(
            vec![level("bitstamp", "101", "1")],
            vec![level("bitstamp", "104", "1")],
        );
        let s = aggregator.merge([&a, &b]);
        assert_eq!(s.bids[0].price, dec("102"));
        assert_eq!(s.bids[1].price, dec("101"));
        assert_eq!(s.bids[2].price, dec("100"));
    }

    #[test]
    fn asks_sorted_ascending() {
        let aggregator = OrderBookAggregator::default();
        let a = book(
            vec![level("binance", "99", "1")],
            vec![level("binance", "105", "1"), level("binance", "103", "1")],
        );
        let b = book(
            vec![level("bitstamp", "98", "1")],
            vec![level("bitstamp", "104", "1")],
        );
        let s = aggregator.merge([&a, &b]);
        assert_eq!(s.asks[0].price, dec("103"));
        assert_eq!(s.asks[1].price, dec("104"));
        assert_eq!(s.asks[2].price, dec("105"));
    }

    #[test]
    fn spread_is_best_ask_minus_best_bid() {
        let aggregator = OrderBookAggregator::default();
        let a = book(
            vec![level("binance", "100", "1")],
            vec![level("binance", "102", "1")],
        );
        let s = aggregator.merge([&a, &OrderBook::default()]);
        assert_eq!(s.spread, dec("2"));
    }

    #[test]
    fn truncates_to_ten_levels() {
        let aggregator = OrderBookAggregator::default();
        let bids: Vec<Level> = (0..15)
            .map(|i| level("binance", &i.to_string(), "1"))
            .collect();
        let a = book(bids, vec![level("binance", "20", "1")]);
        let s = aggregator.merge([&a, &OrderBook::default()]);
        assert_eq!(s.bids.len(), 10);
    }

    #[test]
    fn equal_price_bids_broken_by_amount_descending() {
        let aggregator = OrderBookAggregator::default();
        // Both exchanges quote the same bid price; larger amount should rank first.
        let a = book(vec![level("binance", "100", "1")], vec![]);
        let b = book(vec![level("bitstamp", "100", "5")], vec![]);
        let s = aggregator.merge([&a, &b]);
        assert_eq!(s.bids[0].exchange, "bitstamp"); // 5 > 1
        assert_eq!(s.bids[1].exchange, "binance");
    }

    #[test]
    fn equal_price_asks_broken_by_amount_descending() {
        let aggregator = OrderBookAggregator::default();
        // Both exchanges quote the same ask price; larger amount should rank first.
        let a = book(vec![], vec![level("binance", "101", "2")]);
        let b = book(vec![], vec![level("bitstamp", "101", "8")]);
        let s = aggregator.merge([&a, &b]);
        assert_eq!(s.asks[0].exchange, "bitstamp"); // 8 > 2
        assert_eq!(s.asks[1].exchange, "binance");
    }

    #[test]
    fn empty_books_give_zero_spread() {
        let s =
            OrderBookAggregator::default().merge([&OrderBook::default(), &OrderBook::default()]);
        assert_eq!(s.spread, Decimal::ZERO);
        assert!(s.bids.is_empty());
        assert!(s.asks.is_empty());
    }

    #[test]
    fn custom_max_levels_is_respected() {
        let bids: Vec<Level> = (0..5)
            .map(|i| level("binance", &i.to_string(), "1"))
            .collect();
        let asks: Vec<Level> = (100..105)
            .map(|i| level("binance", &i.to_string(), "1"))
            .collect();

        let s = OrderBookAggregator::new(3).merge([&book(bids, asks)]);
        assert_eq!(s.bids.len(), 3);
        assert_eq!(s.asks.len(), 3);
    }

    #[test]
    fn zero_max_levels_returns_empty_sides_and_zero_spread() {
        let a = book(
            vec![level("binance", "100", "1")],
            vec![level("binance", "101", "1")],
        );
        let s = OrderBookAggregator::new(0).merge([&a]);
        assert_eq!(s.spread, Decimal::ZERO);
        assert!(s.bids.is_empty());
        assert!(s.asks.is_empty());
    }
}
