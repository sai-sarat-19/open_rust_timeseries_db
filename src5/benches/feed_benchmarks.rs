use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ultra_low_latency_feed_v5::{
    MarketDataRecord,
    ZeroAllocRingBuffer,
    rdtsc_timestamp,
};

fn benchmark_ring_buffer(c: &mut Criterion) {
    let ring_buffer = ZeroAllocRingBuffer::<MarketDataRecord>::new(1_048_576);
    
    let record = MarketDataRecord::new(
        1001,   // token
        100.0,  // bid
        100.1,  // ask
        100,    // bid size
        100,    // ask size
        100.05, // last
        50,     // last size
        1,      // seq
        unsafe { rdtsc_timestamp() },
        0,      // flags
    );

    c.bench_function("ring_buffer_write", |b| {
        b.iter(|| {
            unsafe {
                black_box(ring_buffer.write(black_box(&record)));
            }
        })
    });
}

criterion_group!(benches, benchmark_ring_buffer);
criterion_main!(benches); 