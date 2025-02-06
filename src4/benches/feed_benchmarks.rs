use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ultra_low_latency_feed::{
    feed::{FeedMessage, FeedSource, MessageType},
    store::GlobalMarketData,
    init,
};

async fn benchmark_feed_processing(market_data: &GlobalMarketData) {
    let msg = FeedMessage::new(
        1001,   // token
        100.0,  // bid
        100.1,  // ask
        100,    // bid size
        100,    // ask size
        100.05, // last
        50,     // last size
        1,      // seq
        FeedSource::PrimaryExchange,
        MessageType::L1Update,
    );
    
    market_data.process_feed_message(msg).unwrap();
}

fn feed_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let (market_data, _, _) = init().await.unwrap();
        
        c.bench_function("process_feed_message", |b| {
            b.iter(|| {
                rt.block_on(async {
                    benchmark_feed_processing(black_box(&market_data)).await
                })
            })
        });
    });
}

criterion_group!(benches, feed_benchmarks);
criterion_main!(benches); 