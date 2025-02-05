use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};

use ultra_low_latency_db::core::{
    config::BufferConfig,
    market_data::MarketDataRecord,
};
use ultra_low_latency_db::db::ultra_low_latency_db::UltraLowLatencyDB;
use ultra_low_latency_db::memory::zero_alloc_ring_buffer::rdtsc_serialized;

/// Simulates a market data feed system with multiple producers and consumers
#[test]
fn test_market_data_system() {
    println!("\nRunning Market Data System Integration Test");
    println!("==========================================");

    // Configure the system
    let config = BufferConfig {
        buffer_size: 32768,  // 32K records
        max_write_latency: Duration::from_micros(5),   // 5µs max write latency
        max_read_latency: Duration::from_micros(2),    // 2µs max read latency
        use_simd: true,
    };

    // Create market data DB with MarketDataRecord type
    let db = Arc::new(UltraLowLatencyDB::<MarketDataRecord>::new(config.buffer_size));
    
    // Control flag for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    
    // Collect latency statistics
    let mut write_latencies = Vec::with_capacity(1_000_000);
    let mut read_latencies = Vec::with_capacity(1_000_000);

    // Spawn market data producers (simulating different exchanges)
    let mut producer_handles = vec![];
    let num_producers = 4;
    
    for producer_id in 0..num_producers {
        let db = Arc::clone(&db);
        let running = Arc::clone(&running);
        
        let handle = thread::spawn(move || {
            let mut sequence = 0u64;
            let mut local_latencies = Vec::with_capacity(250_000);
            
            while running.load(Ordering::Relaxed) {
                let now = Instant::now();
                let timestamp = now.elapsed().as_nanos() as u64;
                
                // Simulate market data updates
                let record = MarketDataRecord::new(
                    (producer_id * 100 + (sequence % 100)) as u32,  // symbol_id
                    100.0 + (sequence % 100) as f64,    // bid
                    100.1 + (sequence % 100) as f64,    // ask
                    100,                                // bid_size
                    100,                                // ask_size
                    100.05 + (sequence % 100) as f64,   // last
                    100,                                // last_size
                    timestamp,
                    sequence,
                    0,
                );

                // Measure write latency
                let start = unsafe { rdtsc_serialized() };
                while !db.write(&record) {
                    thread::yield_now();
                }
                let end = unsafe { rdtsc_serialized() };
                local_latencies.push(end - start);
                
                sequence += 1;
                
                // Simulate market data arrival rate
                thread::sleep(Duration::from_micros(100));
            }
            
            local_latencies
        });
        
        producer_handles.push(handle);
    }

    // Spawn market data consumers (simulating trading strategies)
    let mut consumer_handles = vec![];
    let num_consumers = 2;
    
    for consumer_id in 0..num_consumers {
        let db = Arc::clone(&db);
        let running = Arc::clone(&running);
        
        let handle = thread::spawn(move || {
            let mut processed = 0u64;
            let mut local_latencies = Vec::with_capacity(500_000);
            
            while running.load(Ordering::Relaxed) {
                // Measure read latency
                let start = unsafe { rdtsc_serialized() };
                if let Some(record) = db.read() {
                    let end = unsafe { rdtsc_serialized() };
                    local_latencies.push(end - start);
                    
                    // Process the market data (simulate trading strategy)
                    if processed % 10_000 == 0 {
                        println!(
                            "Consumer {}: Processed record - Symbol: {}, Bid: {}, Ask: {}, Seq: {}", 
                            consumer_id,
                            record.symbol_id,
                            record.bid_price,
                            record.ask_price,
                            record.sequence_num
                        );
                    }
                    
                    processed += 1;
                } else {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            
            local_latencies
        });
        
        consumer_handles.push(handle);
    }

    // Run the system for 10 seconds
    thread::sleep(Duration::from_secs(10));
    running.store(false, Ordering::SeqCst);

    // Collect and analyze results
    for handle in producer_handles {
        write_latencies.extend(handle.join().unwrap());
    }
    
    for handle in consumer_handles {
        read_latencies.extend(handle.join().unwrap());
    }

    // Analyze latencies
    write_latencies.sort_unstable();
    read_latencies.sort_unstable();

    let write_stats = calculate_latency_stats(&write_latencies);
    let read_stats = calculate_latency_stats(&read_latencies);

    println!("\nSystem Performance Statistics");
    println!("---------------------------");
    println!("Write Latencies (cycles):");
    print_latency_stats(&write_stats);
    println!("\nRead Latencies (cycles):");
    print_latency_stats(&read_stats);

    // Verify latencies are within acceptable bounds
    assert!(
        cycles_to_nanos(write_stats.p99) <= config.max_write_latency.as_nanos() as u64,
        "Write latency P99 exceeds maximum allowed"
    );
    assert!(
        cycles_to_nanos(read_stats.p99) <= config.max_read_latency.as_nanos() as u64,
        "Read latency P99 exceeds maximum allowed"
    );
}

#[derive(Debug)]
struct LatencyStats {
    min: u64,
    max: u64,
    median: u64,
    p90: u64,
    p99: u64,
    p99_9: u64,
}

fn calculate_latency_stats(latencies: &[u64]) -> LatencyStats {
    let len = latencies.len();
    LatencyStats {
        min: latencies[0],
        max: *latencies.last().unwrap(),
        median: latencies[len / 2],
        p90: latencies[(len as f64 * 0.90) as usize],
        p99: latencies[(len as f64 * 0.99) as usize],
        p99_9: latencies[(len as f64 * 0.999) as usize],
    }
}

fn print_latency_stats(stats: &LatencyStats) {
    println!("  Min: {} cycles ({} ns)", stats.min, cycles_to_nanos(stats.min));
    println!("  Median: {} cycles ({} ns)", stats.median, cycles_to_nanos(stats.median));
    println!("  P90: {} cycles ({} ns)", stats.p90, cycles_to_nanos(stats.p90));
    println!("  P99: {} cycles ({} ns)", stats.p99, cycles_to_nanos(stats.p99));
    println!("  P99.9: {} cycles ({} ns)", stats.p99_9, cycles_to_nanos(stats.p99_9));
    println!("  Max: {} cycles ({} ns)", stats.max, cycles_to_nanos(stats.max));
}

// Approximate conversion from CPU cycles to nanoseconds
// This is a rough estimate assuming a 3GHz processor
fn cycles_to_nanos(cycles: u64) -> u64 {
    cycles * 1_000_000_000 / 3_000_000_000
} 