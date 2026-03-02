use rust_decimal::Decimal;

use crate::types::{OrderBook, Summary};

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
    pub fn new(max_levels: usize) -> Self {
        Self { max_levels }
    }

    pub fn merge<'a>(&self, books: impl IntoIterator<Item = &'a OrderBook>) -> Summary {
        !todo!(
            "Implement merging logic to combine multiple order books into a single summary with up to max_levels levels of depth."
        )
    }
}
