use orderbook_lib::{
    aggregator::OrderBookAggregator,
    types::{OrderBook, Summary},
};
use tokio::sync::{broadcast, watch};

pub(crate) trait BookSources {
    async fn changed(&mut self) -> bool;
    fn with_latest<R>(&mut self, f: impl FnOnce(Option<&OrderBook>, Option<&OrderBook>) -> R) -> R;
}

pub(crate) trait SummaryPublisher {
    fn publish(&self, summary: Summary);
}

pub(crate) struct WatchBookSources {
    binance_rx: watch::Receiver<Option<OrderBook>>,
    bitstamp_rx: watch::Receiver<Option<OrderBook>>,
}

impl WatchBookSources {
    pub(crate) fn new(
        binance_rx: watch::Receiver<Option<OrderBook>>,
        bitstamp_rx: watch::Receiver<Option<OrderBook>>,
    ) -> Self {
        Self {
            binance_rx,
            bitstamp_rx,
        }
    }
}

impl BookSources for WatchBookSources {
    async fn changed(&mut self) -> bool {
        tokio::select! {
            result = self.binance_rx.changed() => result.is_ok(),
            result = self.bitstamp_rx.changed() => result.is_ok(),
        }
    }

    fn with_latest<R>(&mut self, f: impl FnOnce(Option<&OrderBook>, Option<&OrderBook>) -> R) -> R {
        let binance = self.binance_rx.borrow_and_update();
        let bitstamp = self.bitstamp_rx.borrow_and_update();
        f(binance.as_ref(), bitstamp.as_ref())
    }
}

pub(crate) struct BroadcastSummaryPublisher {
    tx: broadcast::Sender<Summary>,
}

impl BroadcastSummaryPublisher {
    pub(crate) fn new(tx: broadcast::Sender<Summary>) -> Self {
        Self { tx }
    }
}

impl SummaryPublisher for BroadcastSummaryPublisher {
    fn publish(&self, summary: Summary) {
        let _ = self.tx.send(summary);
    }
}

pub(crate) struct BookSummaryService<S, P> {
    aggregator: OrderBookAggregator,
    sources: S,
    publisher: P,
}

impl<S, P> BookSummaryService<S, P>
where
    S: BookSources + Send + 'static,
    P: SummaryPublisher + Send + 'static,
{
    pub(crate) fn new(aggregator: OrderBookAggregator, sources: S, publisher: P) -> Self {
        Self {
            aggregator,
            sources,
            publisher,
        }
    }

    pub(crate) async fn run(mut self) {
        while self.sources.changed().await {
            let summary = self.sources.with_latest(|binance, bitstamp| {
                if let (Some(b), Some(s)) = (binance, bitstamp) {
                    Some(self.aggregator.merge([b, s]))
                } else {
                    None
                }
            });

            if let Some(summary) = summary {
                self.publisher.publish(summary);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rust_decimal::Decimal;

    use super::*;
    use orderbook_lib::types::Level;

    #[derive(Clone)]
    struct Step {
        changed: bool,
        binance: Option<OrderBook>,
        bitstamp: Option<OrderBook>,
    }

    struct ScriptedSources {
        steps: Vec<Step>,
        index: usize,
        current_binance: Option<OrderBook>,
        current_bitstamp: Option<OrderBook>,
    }

    impl ScriptedSources {
        fn new(steps: Vec<Step>) -> Self {
            Self {
                steps,
                index: 0,
                current_binance: None,
                current_bitstamp: None,
            }
        }
    }

    impl BookSources for ScriptedSources {
        async fn changed(&mut self) -> bool {
            if let Some(step) = self.steps.get(self.index).cloned() {
                self.index += 1;
                self.current_binance = step.binance;
                self.current_bitstamp = step.bitstamp;
                step.changed
            } else {
                false
            }
        }

        fn with_latest<R>(&mut self, f: impl FnOnce(Option<&OrderBook>, Option<&OrderBook>) -> R) -> R {
            f(self.current_binance.as_ref(), self.current_bitstamp.as_ref())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingPublisher {
        published: Arc<Mutex<Vec<Summary>>>,
    }

    impl RecordingPublisher {
        fn snapshot(&self) -> Vec<Summary> {
            self.published.lock().expect("publisher mutex poisoned").clone()
        }
    }

    impl SummaryPublisher for RecordingPublisher {
        fn publish(&self, summary: Summary) {
            self.published.lock().expect("publisher mutex poisoned").push(summary);
        }
    }

    fn book(exchange: &'static str, bid_price: i64, ask_price: i64) -> OrderBook {
        OrderBook {
            bids: vec![Level {
                exchange,
                price: Decimal::from(bid_price),
                amount: Decimal::ONE,
            }]
            .into(),
            asks: vec![Level {
                exchange,
                price: Decimal::from(ask_price),
                amount: Decimal::ONE,
            }]
            .into(),
        }
    }

    #[tokio::test]
    async fn publishes_only_when_both_sources_have_books() {
        let sources = ScriptedSources::new(vec![
            Step {
                changed: true,
                binance: Some(book("binance", 100, 101)),
                bitstamp: None,
            },
            Step {
                changed: true,
                binance: Some(book("binance", 100, 101)),
                bitstamp: Some(book("bitstamp", 99, 102)),
            },
            Step {
                changed: false,
                binance: None,
                bitstamp: None,
            },
        ]);
        let publisher = RecordingPublisher::default();
        let service = BookSummaryService::new(OrderBookAggregator::default(), sources, publisher.clone());

        service.run().await;

        let published = publisher.snapshot();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].spread, Decimal::ONE);
    }

    #[tokio::test]
    async fn publishes_on_each_change_after_initialization() {
        let sources = ScriptedSources::new(vec![
            Step {
                changed: true,
                binance: Some(book("binance", 100, 101)),
                bitstamp: Some(book("bitstamp", 99, 102)),
            },
            Step {
                changed: true,
                binance: Some(book("binance", 100, 100)),
                bitstamp: Some(book("bitstamp", 99, 102)),
            },
            Step {
                changed: false,
                binance: None,
                bitstamp: None,
            },
        ]);
        let publisher = RecordingPublisher::default();
        let service = BookSummaryService::new(OrderBookAggregator::default(), sources, publisher.clone());

        service.run().await;

        let published = publisher.snapshot();
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].spread, Decimal::ONE);
        assert_eq!(published[1].spread, Decimal::ZERO);
    }
}
