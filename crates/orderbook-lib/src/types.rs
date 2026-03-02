use rust_decimal::Decimal;

/// A single price level in an order book.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Level {
    pub exchange: &'static str,
    pub price: Decimal,
    pub quantity: Decimal,
}

/// A snapshot of one exchange's order book.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderBook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// The merged, sorted result that is streamed to gRPC clients.
#[derive(Debug, Clone)]
pub struct Summary {
    /// best_ask - best_bid
    pub spread: Decimal,
    /// Top 10 bids, highest price first.
    pub bids: Vec<Level>,
    /// Top 10 asks, lowest price first.
    pub asks: Vec<Level>,
}
