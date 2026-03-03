fn main() {
    let service = tonic_build::manual::Service::builder()
        .name("OrderbookAggregator")
        .package("orderbook")
        .method(
            tonic_build::manual::Method::builder()
                .name("book_summary")
                .route_name("BookSummary")
                .input_type("super::Empty")
                .output_type("super::Summary")
                .codec_path("super::ProstCodec")
                .server_streaming()
                .build(),
        )
        .build();

    tonic_build::manual::Builder::new()
        .build_client(false)
        .compile(&[service]);
}
