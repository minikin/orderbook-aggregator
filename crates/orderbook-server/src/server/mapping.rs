use orderbook_lib::types::{Level as DomainLevel, Summary as DomainSummary};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tonic::Status;

use super::proto::{Level, Summary as ProtoSummary};

impl TryFrom<DomainSummary> for ProtoSummary {
    type Error = Status;

    fn try_from(s: DomainSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            spread: decimal_to_f64(s.spread).ok_or_else(|| {
                Status::out_of_range(format!("cannot encode summary.spread={} as protobuf double", s.spread))
            })?,
            bids: s.bids.into_iter().map(Level::try_from).collect::<Result<Vec<_>, _>>()?,
            asks: s.asks.into_iter().map(Level::try_from).collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<DomainLevel> for Level {
    type Error = Status;

    fn try_from(l: DomainLevel) -> Result<Self, Self::Error> {
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

#[cfg(test)]
mod tests {
    use orderbook_lib::types::{Level as DomainLevel, Summary as DomainSummary};
    use rust_decimal::Decimal;

    use super::*;

    fn make_summary() -> DomainSummary {
        DomainSummary {
            spread: Decimal::new(1, 3),
            bids: vec![DomainLevel {
                exchange: "binance",
                price: Decimal::new(100, 0),
                amount: Decimal::new(1, 0),
            }]
            .into(),
            asks: vec![DomainLevel {
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
}
