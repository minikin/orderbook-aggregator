mod app;
mod exchange_infra;
mod exchanges;
mod server;

use std::net::SocketAddr;

use anyhow::Context;
use tokio::sync::{broadcast, watch};
use tonic::transport::Server;
use tracing::info;

use app::{BookSummaryService, BroadcastSummaryPublisher, WatchBookSources};
use exchange_infra::ConnectorRuntime;
use exchanges::{binance::BinanceConnector, bitstamp::BitstampConnector};
use orderbook_lib::{aggregator::OrderBookAggregator, types::OrderBook};
use server::{AggregatorService, proto::orderbook_aggregator_server::OrderbookAggregatorServer};

const DEFAULT_PAIR: &str = "ethbtc";
const DEFAULT_PORT: u16 = 50051;
const SUMMARY_BROADCAST_CAPACITY: usize = 64;
const GRPC_CLIENT_BUFFER_CAPACITY: usize = 32;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let pair = DEFAULT_PAIR.to_owned();
    let addr: SocketAddr = format!("0.0.0.0:{DEFAULT_PORT}").parse()?;

    info!("Starting aggregator for pair '{pair}' on {addr}");

    // None = "not yet received"; the aggregator suppresses output until both
    // exchanges have delivered at least one real snapshot. This avoids
    // publishing a Summary computed from one real book + one empty default.
    let (binance_tx, binance_rx) = watch::channel::<Option<OrderBook>>(None);
    let (bitstamp_tx, bitstamp_rx) = watch::channel::<Option<OrderBook>>(None);

    // All connected gRPC clients receive every Summary via broadcast.
    let (summary_tx, _) = broadcast::channel(SUMMARY_BROADCAST_CAPACITY);

    // Store handles so a connector panic is detected and causes a clean exit
    // rather than silently degrading to a single-exchange view.
    let binance_handle =
        tokio::spawn(ConnectorRuntime::new(BinanceConnector, pair.clone(), binance_tx).run());
    let bitstamp_handle =
        tokio::spawn(ConnectorRuntime::new(BitstampConnector, pair.clone(), bitstamp_tx).run());

    // Application service: reacts whenever either exchange book changes.
    // Only publishes once both exchanges have sent their first snapshot.
    let summary_service = BookSummaryService::new(
        OrderBookAggregator::default(),
        WatchBookSources::new(binance_rx, bitstamp_rx),
        BroadcastSummaryPublisher::new(summary_tx.clone()),
    );
    let aggregator = tokio::spawn(summary_service.run());

    let service = AggregatorService {
        summary_tx,
        client_buffer_capacity: GRPC_CLIENT_BUFFER_CAPACITY,
    };

    info!("gRPC server listening on {addr}");

    // If the aggregator exits (e.g. due to a panic), shut down the entire
    // process rather than silently serving a stale stream to gRPC clients.
    tokio::select! {
        result = Server::builder()
            .add_service(OrderbookAggregatorServer::new(service))
            .serve_with_shutdown(addr, async {
                // Wait for SIGINT (Ctrl-C); give in-flight RPCs a chance to finish.
                tokio::signal::ctrl_c().await.ok();
                info!("received SIGINT, shutting down gracefully");
            }) => {
            result.context("gRPC server error")?;
        }
        result = aggregator => {
            match result {
                Ok(()) => anyhow::bail!("aggregator task exited unexpectedly"),
                Err(e) => anyhow::bail!("aggregator task panicked: {e}"),
            }
        }
        result = binance_handle => {
            match result {
                Ok(Ok(())) => anyhow::bail!("binance connector exited unexpectedly"),
                Ok(Err(e)) => anyhow::bail!("binance connector exited with fatal error: {e}"),
                Err(e) => anyhow::bail!("binance connector panicked: {e}"),
            }
        }
        result = bitstamp_handle => {
            match result {
                Ok(Ok(())) => anyhow::bail!("bitstamp connector exited unexpectedly"),
                Ok(Err(e)) => anyhow::bail!("bitstamp connector exited with fatal error: {e}"),
                Err(e) => anyhow::bail!("bitstamp connector panicked: {e}"),
            }
        }
    }

    Ok(())
}
