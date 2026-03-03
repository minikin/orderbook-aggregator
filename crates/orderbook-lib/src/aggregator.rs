use rust_decimal::Decimal;

use std::cmp::Ordering;

use crate::types::{Level, Levels, OrderBook, Summary};

/// Merger configuration for constructing merged summaries.
#[derive(Debug, Clone)]
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

        Self::keep_top_n(&mut bids, self.max_levels, Self::bids_cmp);
        Self::keep_top_n(&mut asks, self.max_levels, Self::asks_cmp);

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
}
