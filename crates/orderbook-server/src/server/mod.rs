use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

mod mapping;
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/orderbook.rs"));
}

use proto::{Empty, Summary as ProtoSummary, orderbook_aggregator_server::OrderbookAggregator};

pub struct AggregatorService {
    pub summary_tx: broadcast::Sender<ProtoSummary>,
    pub client_buffer_capacity: usize,
}

#[tonic::async_trait]
impl OrderbookAggregator for AggregatorService {
    type BookSummaryStream = ReceiverStream<Result<ProtoSummary, Status>>;

    async fn book_summary(&self, _request: Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let mut rx = self.summary_tx.subscribe();

        // Bridge the broadcast receiver into an mpsc channel so we can wrap it
        // in ReceiverStream. The task exits when the client disconnects
        // (send returns Err) or the broadcast channel closes.
        //
        // A slow client stalls its bridge task after
        // `self.client_buffer_capacity` pending items; the
        // bridge then falls behind on broadcast, and the lag detector fires
        // before the broadcast buffer is full. Tune both values together.
        let (tx, rx_stream) = mpsc::channel(self.client_buffer_capacity);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(summary) => {
                        if tx.send(Ok(summary)).await.is_err() {
                            break; // client disconnected
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("gRPC client lagged by {n} messages, disconnecting");
                        // Send an explicit error so the client knows it was
                        // dropped for being slow, rather than seeing a clean
                        // end-of-stream it might silently ignore.
                        let _ = tx
                            .send(Err(Status::resource_exhausted(format!(
                                "client lagged by {n} messages and was disconnected"
                            ))))
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx_stream)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::broadcast;
    use tokio_stream::StreamExt as _;
    use tonic::Request;

    use super::*;

    fn make_proto_summary() -> ProtoSummary {
        ProtoSummary {
            spread: 0.001,
            bids: vec![proto::Level {
                exchange: "binance".to_owned(),
                price: 100.0,
                amount: 1.0,
            }],
            asks: vec![proto::Level {
                exchange: "bitstamp".to_owned(),
                price: 100.001,
                amount: 2.0,
            }],
        }
    }

    #[tokio::test]
    async fn streams_summary_to_connected_client() {
        let (tx, _) = broadcast::channel::<ProtoSummary>(16);
        let service = AggregatorService {
            summary_tx: tx.clone(),
            client_buffer_capacity: 8,
        };

        let mut stream = service.book_summary(Request::new(Empty {})).await.unwrap().into_inner();

        tx.send(make_proto_summary()).unwrap();

        let proto = stream.next().await.unwrap().unwrap();
        assert!((proto.spread - 0.001).abs() < 1e-10);
        assert_eq!(proto.bids[0].exchange, "binance");
        assert_eq!(proto.asks[0].exchange, "bitstamp");
    }

    #[tokio::test]
    async fn closes_stream_when_broadcast_channel_drops() {
        let (tx, _) = broadcast::channel::<ProtoSummary>(16);
        let service = AggregatorService {
            summary_tx: tx,
            client_buffer_capacity: 8,
        };

        let mut stream = service.book_summary(Request::new(Empty {})).await.unwrap().into_inner();

        drop(service);

        let result = tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("stream should close within 100ms");

        assert!(result.is_none(), "stream should terminate when broadcast closes");
    }
}
