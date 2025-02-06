use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};

use ultra_low_latency_db::core::{
    market_data::MarketDataRecord,
    instrument_index::InstrumentBufferConfig,
};
use ultra_low_latency_db::memory::{
    instrument_buffer::{InstrumentBufferManager, BufferType},
    zero_alloc_ring_buffer::rdtsc_serialized,
};

#[test]
fn test_instrument_buffer_system() {
    println!("\nRunning Instrument Buffer System Test");
    println!("====================================");

    // Configure the system
    let config = InstrumentBufferConfig {
        l1_buffer_size: 65536,  // 64K for price updates
        l2_buffer_size: 32768,  // 32K for trades
        ref_buffer_size: 8192,  // 8K for reference data
    };

    // Create buffer manager with capacity for 1000 instruments
    let mut manager = InstrumentBufferManager::<MarketDataRecord>::new(1000, config);

    // Register some test instruments
    let instruments = vec![1001, 1002, 1003, 1004];
    for &token in &instruments {
        assert!(manager.register_instrument(token).is_some());
    }

    // Control flag for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    
    // Collect latency statistics
    let mut write_latencies = Vec::with_capacity(1_000_000);
    let mut read_latencies = Vec::with_capacity(1_000_000);

    // Spawn market data producers (simulating different exchanges)
    let mut producer_handles = vec![];
    let manager = Arc::new(manager);
    
    for instrument in &instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let token = *instrument;
        
        let handle = thread::spawn(move || {
            let mut sequence = 0u64;
            let mut local_latencies = Vec::with_capacity(250_000);
            
            while running.load(Ordering::Relaxed) {
                let now = Instant::now();
                let timestamp = now.elapsed().as_nanos() as u64;
                
                // Create L1 price update
                let record = MarketDataRecord::new(
                    token,
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
                unsafe {
                    while !manager.write(token, &record, BufferType::L1Price) {
                        thread::yield_now();
                    }
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
    
    for instrument in &instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let token = *instrument;
        
        let handle = thread::spawn(move || {
            let mut processed = 0u64;
            let mut local_latencies = Vec::with_capacity(500_000);
            
            while running.load(Ordering::Relaxed) {
                // Measure read latency
                let start = unsafe { rdtsc_serialized() };
                unsafe {
                    if let Some(record) = manager.read(token, BufferType::L1Price) {
                        let end = rdtsc_serialized();
                        let latency = end - start;
                        local_latencies.push(latency);
                        
                        // Process the market data (simulate trading strategy)
                        if processed % 10_000 == 0 {
                            println!(
                                "Consumer for instrument {}: Processed record - Bid: {}, Ask: {}, Seq: {}", 
                                token,
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

    // Verify the system is working correctly
    for &token in &instruments {
        unsafe {
            // Should be able to read from all buffer types
            assert!(manager.read(token, BufferType::L1Price).is_some());
            assert!(manager.read(token, BufferType::L2Trade).is_none()); // No trades written
            assert!(manager.read(token, BufferType::Reference).is_none()); // No reference data written
        }
    }
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