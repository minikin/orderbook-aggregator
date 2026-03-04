use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use orderbook_lib::types::Summary;
use rust_decimal::{Decimal, prelude::ToPrimitive};

pub mod proto {
    use std::marker::PhantomData;

    use prost::Message;
    use tonic::Status;
    use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

    #[derive(Clone, PartialEq, Message)]
    pub struct Empty {}

    #[derive(Clone, PartialEq, Message)]
    pub struct Level {
        #[prost(string, tag = "1")]
        pub exchange: String,

        #[prost(double, tag = "2")]
        pub price: f64,

        #[prost(double, tag = "3")]
        pub amount: f64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Summary {
        #[prost(double, tag = "1")]
        pub spread: f64,

        #[prost(message, repeated, tag = "2")]
        pub bids: Vec<Level>,

        #[prost(message, repeated, tag = "3")]
        pub asks: Vec<Level>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct ProstCodec<T, U> {
        _pd: PhantomData<(T, U)>,
    }

    #[derive(Debug, Clone)]
    pub struct ProstEncoder<T> {
        _pd: PhantomData<T>,
        buffer_settings: BufferSettings,
    }

    #[derive(Debug, Clone)]
    pub struct ProstDecoder<U> {
        _pd: PhantomData<U>,
        buffer_settings: BufferSettings,
    }

    impl<T, U> Codec for ProstCodec<T, U>
    where
        T: Message + Send + 'static,
        U: Message + Default + Send + 'static,
    {
        type Encode = T;
        type Decode = U;
        type Encoder = ProstEncoder<T>;
        type Decoder = ProstDecoder<U>;

        fn encoder(&mut self) -> Self::Encoder {
            ProstEncoder {
                _pd: PhantomData,
                buffer_settings: BufferSettings::default(),
            }
        }

        fn decoder(&mut self) -> Self::Decoder {
            ProstDecoder {
                _pd: PhantomData,
                buffer_settings: BufferSettings::default(),
            }
        }
    }

    impl<T: Message> Encoder for ProstEncoder<T> {
        type Item = T;
        type Error = Status;

        fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
            item.encode(buf)
                .expect("prost message encode only fails when out of buffer space");
            Ok(())
        }

        fn buffer_settings(&self) -> BufferSettings {
            self.buffer_settings
        }
    }

    impl<U: Message + Default> Decoder for ProstDecoder<U> {
        type Item = U;
        type Error = Status;

        fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
            Message::decode(buf)
                .map(Some)
                .map_err(|e| Status::internal(e.to_string()))
        }

        fn buffer_settings(&self) -> BufferSettings {
            self.buffer_settings
        }
    }

    include!(concat!(env!("OUT_DIR"), "/orderbook.OrderbookAggregator.rs"));
}

use proto::{Empty, Level, Summary as ProtoSummary, orderbook_aggregator_server::OrderbookAggregator};

impl TryFrom<Summary> for ProtoSummary {
    type Error = Status;

    fn try_from(s: Summary) -> Result<Self, Self::Error> {
        Ok(Self {
            spread: decimal_to_f64(s.spread).ok_or_else(|| {
                Status::out_of_range(format!("cannot encode summary.spread={} as protobuf double", s.spread))
            })?,
            bids: s.bids.into_iter().map(Level::try_from).collect::<Result<Vec<_>, _>>()?,
            asks: s.asks.into_iter().map(Level::try_from).collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<orderbook_lib::types::Level> for Level {
    type Error = Status;

    fn try_from(l: orderbook_lib::types::Level) -> Result<Self, Self::Error> {
        Ok(Self {
            exchange: l.exchange.to_owned(),
            price: decimal_to_f64(l.price).ok_or_else(|| {
                Status::out_of_range(format!("cannot encode level.price={} as protobuf double", l.price))
            })?,
            amount: decimal_to_f64(l.amount).ok_or_else(|| {
                Status::out_of_range(format!("cannot encode level.amount={} as protobuf double", l.amount))
            })?,
        })
    }
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_f64()
}

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

    use orderbook_lib::types::{Level, Summary};
    use rust_decimal::Decimal;
    use tokio::sync::broadcast;
    use tokio_stream::StreamExt as _;
    use tonic::Request;

    use super::*;

    fn make_summary() -> Summary {
        Summary {
            spread: Decimal::new(1, 3),
            bids: vec![Level {
                exchange: "binance",
                price: Decimal::new(100, 0),
                amount: Decimal::new(1, 0),
            }]
            .into(),
            asks: vec![Level {
                exchange: "bitstamp",
                price: Decimal::new(100001, 3),
                amount: Decimal::new(2, 0),
            }]
            .into(),
        }
    }

    #[test]
    fn proto_summary_try_from_maps_values() {
        let proto = ProtoSummary::try_from(make_summary()).expect("summary should convert");
        assert!((proto.spread - 0.001).abs() < 1e-10);
        assert_eq!(proto.bids.len(), 1);
        assert_eq!(proto.asks.len(), 1);
        assert_eq!(proto.bids[0].exchange, "binance");
        assert_eq!(proto.asks[0].exchange, "bitstamp");
    }

    #[test]
    fn decimal_to_f64_converts_decimal_extremes() {
        assert!(decimal_to_f64(Decimal::MAX).is_some());
        assert!(decimal_to_f64(Decimal::MIN).is_some());
    }

    #[tokio::test]
    async fn streams_summary_to_connected_client() {
        let (tx, _) = broadcast::channel::<ProtoSummary>(16);
        let service = AggregatorService {
            summary_tx: tx.clone(),
            client_buffer_capacity: 8,
        };

        let mut stream = service.book_summary(Request::new(Empty {})).await.unwrap().into_inner();

        tx.send(ProtoSummary::try_from(make_summary()).unwrap()).unwrap();

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
