use rust_decimal::Decimal;
use smallvec::SmallVec;

/// Inline storage capacity for order-book sides before spilling to heap.
pub const INLINE_LEVEL_CAPACITY: usize = 24;
pub type Levels = SmallVec<[Level; INLINE_LEVEL_CAPACITY]>;

/// A single price level in an order book.
#[derive(Debug, Clone)]
pub struct Level {
    pub exchange: &'static str,
    pub price: Decimal,
    pub amount: Decimal,
}

/// A snapshot of one exchange's order book (normalised representation).
#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    pub bids: Levels,
    pub asks: Levels,
}

/// The merged, sorted result that is streamed to gRPC clients.
#[derive(Debug, Clone)]
pub struct Summary {
    /// best_ask - best_bid
    pub spread: Decimal,
    /// Top 10 bids, highest price first.
    pub bids: Levels,
    /// Top 10 asks, lowest price first.
    pub asks: Levels,
}
