mod app;
mod exchange_infra;
mod exchanges;
mod server;

use std::net::SocketAddr;

use anyhow::{Context, anyhow};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{info, warn};

use app::{BookSummaryService, BroadcastSummaryPublisher, WatchBookSources};
use exchange_infra::{ConnectorRuntime, ExchangeError};
use exchanges::{binance::BinanceConnector, bitstamp::BitstampConnector};
use orderbook_lib::{
    aggregator::OrderBookAggregator,
    types::{OrderBook, Summary},
};
use server::{
    AggregatorService,
    proto::{Summary as ProtoSummary, orderbook_aggregator_server::OrderbookAggregatorServer},
};

const DEFAULT_PAIR: &str = "ethbtc";
const DEFAULT_PORT: u16 = 50051;
const SUMMARY_BROADCAST_CAPACITY: usize = 64;
const GRPC_CLIENT_BUFFER_CAPACITY: usize = 32;

struct RuntimeHandles {
    aggregator: JoinHandle<()>,
    binance: JoinHandle<Result<(), ExchangeError>>,
    bitstamp: JoinHandle<Result<(), ExchangeError>>,
    summary_to_proto: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    run().await
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
}

async fn run() -> anyhow::Result<()> {
    let pair = DEFAULT_PAIR.to_owned();
    let addr = default_bind_addr()?;

    info!("Starting aggregator for pair '{pair}' on {addr}");

    let (service, handles) = build_runtime(pair);

    info!("gRPC server listening on {addr}");

    run_until_exit(addr, service, handles).await
}

fn default_bind_addr() -> anyhow::Result<SocketAddr> {
    format!("0.0.0.0:{DEFAULT_PORT}")
        .parse()
        .context("failed to parse server bind address")
}

fn build_runtime(pair: String) -> (AggregatorService, RuntimeHandles) {
    // None = "not yet received"; the aggregator suppresses output until both
    // exchanges have delivered at least one real snapshot. This avoids
    // publishing a Summary computed from one real book + one empty default.
    let (binance_tx, binance_rx) = watch::channel::<Option<OrderBook>>(None);
    let (bitstamp_tx, bitstamp_rx) = watch::channel::<Option<OrderBook>>(None);

    // Internal fanout of merged domain summaries from app layer.
    let (summary_tx, _) = broadcast::channel::<Summary>(SUMMARY_BROADCAST_CAPACITY);
    // gRPC fanout of protobuf summaries; conversion is done once per update.
    let (proto_summary_tx, _) = broadcast::channel::<ProtoSummary>(SUMMARY_BROADCAST_CAPACITY);

    let handles = RuntimeHandles {
        // Store handles so a connector panic is detected and causes a clean
        // exit rather than silently degrading to a single-exchange view.
        binance: tokio::spawn(ConnectorRuntime::new(BinanceConnector, pair.clone(), binance_tx).run()),
        bitstamp: tokio::spawn(ConnectorRuntime::new(BitstampConnector, pair.clone(), bitstamp_tx).run()),
        // Application service: reacts whenever either exchange book changes.
        // Only publishes once both exchanges have sent their first snapshot.
        aggregator: spawn_summary_aggregator(binance_rx, bitstamp_rx, summary_tx.clone()),
        summary_to_proto: spawn_summary_to_proto_converter(&summary_tx, proto_summary_tx.clone()),
    };

    let service = AggregatorService {
        summary_tx: proto_summary_tx,
        client_buffer_capacity: GRPC_CLIENT_BUFFER_CAPACITY,
    };

    (service, handles)
}

fn spawn_summary_aggregator(
    binance_rx: watch::Receiver<Option<OrderBook>>,
    bitstamp_rx: watch::Receiver<Option<OrderBook>>,
    summary_tx: broadcast::Sender<Summary>,
) -> JoinHandle<()> {
    let summary_service = BookSummaryService::new(
        OrderBookAggregator::default(),
        WatchBookSources::new(binance_rx, bitstamp_rx),
        BroadcastSummaryPublisher::new(summary_tx),
    );
    tokio::spawn(summary_service.run())
}

fn spawn_summary_to_proto_converter(
    summary_tx: &broadcast::Sender<Summary>,
    proto_summary_tx: broadcast::Sender<ProtoSummary>,
) -> JoinHandle<()> {
    let mut summary_rx = summary_tx.subscribe();
    tokio::spawn(async move {
        loop {
            match summary_rx.recv().await {
                Ok(summary) => match ProtoSummary::try_from(summary) {
                    Ok(proto) => {
                        let _ = proto_summary_tx.send(proto);
                    }
                    Err(status) => {
                        warn!(
                            "dropping summary that could not be encoded to protobuf: {}",
                            status.message()
                        );
                    }
                },
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("summary->proto converter lagged by {n} messages");
                }
            }
        }
    })
}

async fn run_until_exit(addr: SocketAddr, service: AggregatorService, handles: RuntimeHandles) -> anyhow::Result<()> {
    let RuntimeHandles {
        aggregator,
        binance,
        bitstamp,
        summary_to_proto,
    } = handles;

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
        result = aggregator => return Err(task_exit_error("aggregator", result)),
        result = binance => return Err(connector_exit_error("binance", result)),
        result = bitstamp => return Err(connector_exit_error("bitstamp", result)),
        result = summary_to_proto => return Err(task_exit_error("summary->proto converter", result)),
    }

    Ok(())
}

fn task_exit_error(name: &str, result: Result<(), tokio::task::JoinError>) -> anyhow::Error {
    match result {
        Ok(()) => anyhow!("{name} task exited unexpectedly"),
        Err(e) => anyhow!("{name} task panicked: {e}"),
    }
}

fn connector_exit_error(
    name: &str,
    result: Result<Result<(), ExchangeError>, tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(Ok(())) => anyhow!("{name} connector exited unexpectedly"),
        Ok(Err(e)) => anyhow!("{name} connector exited with fatal error: {e}"),
        Err(e) => anyhow!("{name} connector panicked: {e}"),
    }
}
