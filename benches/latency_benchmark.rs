use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;
use ultra_low_latency_db::{
    core::record::Record,
    core::types::*,
    memory::ring_buffer::RingBuffer,
    engine::db::Database,
};

fn benchmark_ring_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(1000);

    let ring = RingBuffer::new(16384);
    let record = Record::with_current_time(1, 100, 1234.56, 1000, 0);

    // Benchmark single write
    group.bench_function("write", |b| {
        b.iter(|| {
            unsafe {
                black_box(ring.write(black_box(&record)));
            }
        });
    });

    // Benchmark single read
    group.bench_function("read", |b| {
        b.iter(|| {
            unsafe {
                black_box(ring.read());
            }
        });
    });

    // Benchmark write-read cycle
    group.bench_function("write_read_cycle", |b| {
        b.iter(|| {
            unsafe {
                ring.write(black_box(&record));
                black_box(ring.read());
            }
        });
    });

    group.finish();
}

fn benchmark_database(c: &mut Criterion) {
    let mut group = c.benchmark_group("database");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(1000);

    let db = Database::default();

    // Benchmark single symbol write
    group.bench_function("write_single_symbol", |b| {
        b.iter(|| {
            black_box(db.write(black_box(1), black_box(100.0), black_box(1000)));
        });
    });

    // Benchmark multi-symbol write
    group.bench_function("write_multi_symbol", |b| {
        let mut symbol_id = 0;
        b.iter(|| {
            symbol_id = (symbol_id + 1) % 100;
            black_box(db.write(
                black_box(symbol_id),
                black_box(100.0),
                black_box(1000)
            ));
        });
    });

    // Benchmark read latest
    group.bench_function("read_latest", |b| {
        b.iter(|| {
            black_box(db.read_latest(black_box(1)));
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_ring_buffer, benchmark_database);
criterion_main!(benches); 