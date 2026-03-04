use std::marker::PhantomData;

use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

include!(concat!(env!("OUT_DIR"), "/orderbook.OrderbookAggregator.rs"));

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
