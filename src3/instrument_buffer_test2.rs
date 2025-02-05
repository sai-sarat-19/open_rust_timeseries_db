use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;

use crate::core::{
    market_data::MarketDataRecord,
    instrument_index::InstrumentBufferConfig,
};
use crate::memory::{
    instrument_buffer::{InstrumentBufferManager, BufferType},
    zero_alloc_ring_buffer::rdtsc_serialized,
};

// Simulates different market data sources
enum MarketDataSource {
    PrimaryExchange,   // High frequency, reliable
    SecondaryVenue,    // Medium frequency, occasional delays
    DarkPool,          // Low frequency, sporadic
    ReferenceData,     // Very low frequency, batch updates
}

// Simulates different consumer types
enum ConsumerType {
    AlgoTrading,      // Needs every tick
    RiskManagement,   // Periodic snapshots
    Compliance,       // Batch processing
    Analytics,        // Sampling
}

struct ProducerStats {
    total_attempts: u64,
    successful_writes: u64,
    buffer_full_count: u64,
    validation_failures: u64,
    total_latency: u64,
}

struct ConsumerStats {
    total_attempts: u64,
    successful_reads: u64,
    buffer_empty_count: u64,
    total_latency: u64,
    missed_updates: u64,
}

#[test]
fn test_production_buffer_system() {
    println!("\nRunning Production Buffer System Test");
    println!("====================================");

    // Configure larger buffers for production load
    let config = InstrumentBufferConfig {
        l1_buffer_size: 1_048_576,  // 1M for L1 (high frequency)
        l2_buffer_size: 524_288,    // 512K for L2 (medium frequency)
        ref_buffer_size: 65_536,    // 64K for reference (low frequency)
    };

    // Create buffer manager with capacity for 10,000 instruments
    let mut manager = InstrumentBufferManager::<MarketDataRecord>::new(10_000, config);

    // Register test instruments (different ranges for different venues)
    let primary_instruments = (1000..1100).collect::<Vec<_>>();    // 100 primary instruments
    let secondary_instruments = (2000..2500).collect::<Vec<_>>();  // 500 secondary instruments
    let darkpool_instruments = (3000..4000).collect::<Vec<_>>();   // 1000 darkpool instruments
    
    let all_instruments = [
        primary_instruments.clone(),
        secondary_instruments.clone(),
        darkpool_instruments.clone(),
    ].concat();

    // Register all instruments
    for &token in &all_instruments {
        assert!(manager.register_instrument(token).is_some());
    }

    // Control flags
    let running = Arc::new(AtomicBool::new(true));
    let error_injection = Arc::new(AtomicBool::new(false));
    
    // Shared statistics
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));

    let manager = Arc::new(manager);
    let mut producer_handles = vec![];
    let mut consumer_handles = vec![];
    
    // Spawn different types of producers
    
    // 1. Primary Exchange - High frequency L1 updates
    for &token in &primary_instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let error_injection = Arc::clone(&error_injection);
        let total_writes = Arc::clone(&total_writes);
        
        let handle = thread::spawn(move || {
            let mut stats = ProducerStats {
                total_attempts: 0,
                successful_writes: 0,
                buffer_full_count: 0,
                validation_failures: 0,
                total_latency: 0,
            };
            
            let mut sequence = 0u64;
            let mut last_price = 100.0;
            
            while running.load(Ordering::Relaxed) {
                stats.total_attempts += 1;
                
                // Simulate price movement
                last_price += (sequence % 3) as f64 * 0.01 * if sequence % 2 == 0 { 1.0 } else { -1.0 };
                
                let record = MarketDataRecord::new(
                    token,
                    last_price - 0.01,        // bid
                    last_price + 0.01,        // ask
                    100 + (sequence % 50) as u32,  // bid_size
                    100 + (sequence % 45) as u32,  // ask_size
                    last_price,               // last_price
                    100,                      // last_size
                    Instant::now().elapsed().as_nanos() as u64,
                    sequence,
                    0,
                );

                // Inject artificial errors occasionally
                if error_injection.load(Ordering::Relaxed) && sequence % 1000 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }

                let start = unsafe { rdtsc_serialized() };
                let result = unsafe {
                    manager.write(token, &record, BufferType::L1Price)
                };
                let end = unsafe { rdtsc_serialized() };
                
                if result {
                    stats.successful_writes += 1;
                    stats.total_latency += end - start;
                    total_writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.buffer_full_count += 1;
                    thread::yield_now();
                    continue;
                }
                
                sequence += 1;
                
                // High frequency updates - 100 microsecond intervals
                thread::sleep(Duration::from_micros(100));
            }
            
            stats
        });
        
        producer_handles.push(handle);
    }

    // 2. Secondary Venue - Medium frequency L1 and L2 updates
    for &token in &secondary_instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let total_writes = Arc::clone(&total_writes);
        
        let handle = thread::spawn(move || {
            let mut stats = ProducerStats {
                total_attempts: 0,
                successful_writes: 0,
                buffer_full_count: 0,
                validation_failures: 0,
                total_latency: 0,
            };
            
            let mut sequence = 0u64;
            let mut last_price = 100.0;
            
            while running.load(Ordering::Relaxed) {
                stats.total_attempts += 1;
                
                // Simulate more volatile price movement
                last_price += (sequence % 5) as f64 * 0.02 * if sequence % 2 == 0 { 1.0 } else { -1.0 };
                
                // L1 Update
                let l1_record = MarketDataRecord::new(
                    token,
                    last_price - 0.02,
                    last_price + 0.02,
                    200 + (sequence % 100) as u32,
                    200 + (sequence % 90) as u32,
                    last_price,
                    200,
                    Instant::now().elapsed().as_nanos() as u64,
                    sequence,
                    0,
                );

                let start = unsafe { rdtsc_serialized() };
                let result = unsafe {
                    manager.write(token, &l1_record, BufferType::L1Price)
                };
                let end = unsafe { rdtsc_serialized() };
                
                if result {
                    stats.successful_writes += 1;
                    stats.total_latency += end - start;
                    total_writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.buffer_full_count += 1;
                }

                // L2 Update (every 5th tick)
                if sequence % 5 == 0 {
                    let l2_record = MarketDataRecord::new(
                        token,
                        last_price,
                        last_price,
                        500,
                        500,
                        last_price,
                        500,
                        Instant::now().elapsed().as_nanos() as u64,
                        sequence,
                        1, // Different flag for L2
                    );

                    if unsafe { manager.write(token, &l2_record, BufferType::L2Trade) } {
                        stats.successful_writes += 1;
                        total_writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                sequence += 1;
                
                // Medium frequency - 500 microsecond intervals
                thread::sleep(Duration::from_micros(500));
            }
            
            stats
        });
        
        producer_handles.push(handle);
    }

    // 3. Dark Pool - Sporadic L2 trade updates
    for &token in &darkpool_instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let total_writes = Arc::clone(&total_writes);
        
        let handle = thread::spawn(move || {
            let mut stats = ProducerStats {
                total_attempts: 0,
                successful_writes: 0,
                buffer_full_count: 0,
                validation_failures: 0,
                total_latency: 0,
            };
            
            let mut sequence = 0u64;
            let mut last_price = 100.0;
            
            while running.load(Ordering::Relaxed) {
                stats.total_attempts += 1;
                
                // Simulate large trades
                last_price += (sequence % 10) as f64 * 0.05 * if sequence % 2 == 0 { 1.0 } else { -1.0 };
                
                let record = MarketDataRecord::new(
                    token,
                    last_price,
                    last_price,
                    1000 + (sequence % 1000) as u32,
                    1000 + (sequence % 1000) as u32,
                    last_price,
                    1000,
                    Instant::now().elapsed().as_nanos() as u64,
                    sequence,
                    2, // Dark pool flag
                );

                let start = unsafe { rdtsc_serialized() };
                let result = unsafe {
                    manager.write(token, &record, BufferType::L2Trade)
                };
                let end = unsafe { rdtsc_serialized() };
                
                if result {
                    stats.successful_writes += 1;
                    stats.total_latency += end - start;
                    total_writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.buffer_full_count += 1;
                }
                
                sequence += 1;
                
                // Sporadic updates - random intervals between 1-5ms
                thread::sleep(Duration::from_millis(1 + (sequence % 4)));
            }
            
            stats
        });
        
        producer_handles.push(handle);
    }

    // Spawn different types of consumers
    
    // 1. Algo Trading - Needs every tick from primary instruments
    for &token in &primary_instruments {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let total_reads = Arc::clone(&total_reads);
        
        let handle = thread::spawn(move || {
            let mut stats = ConsumerStats {
                total_attempts: 0,
                successful_reads: 0,
                buffer_empty_count: 0,
                total_latency: 0,
                missed_updates: 0,
            };
            
            let mut last_seq = 0u64;
            
            while running.load(Ordering::Relaxed) {
                stats.total_attempts += 1;
                
                let start = unsafe { rdtsc_serialized() };
                let result = unsafe {
                    manager.read(token, BufferType::L1Price)
                };
                let end = unsafe { rdtsc_serialized() };
                
                if let Some(record) = result {
                    stats.successful_reads += 1;
                    stats.total_latency += end - start;
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    
                    // Check for missed updates
                    if record.sequence_num > last_seq + 1 && last_seq > 0 {
                        stats.missed_updates += record.sequence_num - last_seq - 1;
                    }
                    last_seq = record.sequence_num;
                    
                    // Process tick (simulate algo trading)
                    if stats.successful_reads % 10_000 == 0 {
                        println!(
                            "Algo Trading - Instrument {}: Processed tick {} - Price: {}", 
                            token,
                            record.sequence_num,
                            record.last_price
                        );
                    }
                } else {
                    stats.buffer_empty_count += 1;
                    thread::sleep(Duration::from_micros(10));
                }
            }
            
            stats
        });
        
        consumer_handles.push(handle);
    }

    // 2. Risk Management - Periodic snapshots of all instruments
    {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let total_reads = Arc::clone(&total_reads);
        let all_instruments = all_instruments.clone();
        
        let handle = thread::spawn(move || {
            let mut stats = ConsumerStats {
                total_attempts: 0,
                successful_reads: 0,
                buffer_empty_count: 0,
                total_latency: 0,
                missed_updates: 0,
            };
            
            let mut position_map: HashMap<u32, f64> = HashMap::new();
            
            while running.load(Ordering::Relaxed) {
                let snapshot_start = Instant::now();
                let mut total_position = 0.0;
                
                // Take snapshot of all instruments
                for &token in &all_instruments {
                    stats.total_attempts += 1;
                    
                    let start = unsafe { rdtsc_serialized() };
                    let result = unsafe {
                        if token < 2000 {
                            manager.read(token, BufferType::L1Price)
                        } else {
                            manager.read(token, BufferType::L2Trade)
                        }
                    };
                    let end = unsafe { rdtsc_serialized() };
                    
                    if let Some(record) = result {
                        stats.successful_reads += 1;
                        stats.total_latency += end - start;
                        total_reads.fetch_add(1, Ordering::Relaxed);
                        
                        // Update position
                        let position = record.last_price * record.last_size as f64;
                        position_map.insert(token, position);
                        total_position += position;
                    }
                }
                
                if stats.successful_reads % 1000 == 0 {
                    println!(
                        "Risk Management - Total Position: {:.2}, Snapshot Time: {:?}", 
                        total_position,
                        snapshot_start.elapsed()
                    );
                }
                
                // Risk checks every 100ms
                thread::sleep(Duration::from_millis(100));
            }
            
            stats
        });
        
        consumer_handles.push(handle);
    }

    // 3. Analytics - Sampling based consumption
    {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let total_reads = Arc::clone(&total_reads);
        let secondary_instruments = secondary_instruments.clone();
        
        let handle = thread::spawn(move || {
            let mut stats = ConsumerStats {
                total_attempts: 0,
                successful_reads: 0,
                buffer_empty_count: 0,
                total_latency: 0,
                missed_updates: 0,
            };
            
            let mut vwap_map: HashMap<u32, (f64, u32)> = HashMap::new(); // (price*volume, volume)
            
            while running.load(Ordering::Relaxed) {
                for &token in &secondary_instruments {
                    stats.total_attempts += 1;
                    
                    let start = unsafe { rdtsc_serialized() };
                    let result = unsafe {
                        manager.read(token, BufferType::L2Trade)
                    };
                    let end = unsafe { rdtsc_serialized() };
                    
                    if let Some(record) = result {
                        stats.successful_reads += 1;
                        stats.total_latency += end - start;
                        total_reads.fetch_add(1, Ordering::Relaxed);
                        
                        // Calculate VWAP
                        let entry = vwap_map.entry(token).or_insert((0.0, 0));
                        entry.0 += record.last_price * record.last_size as f64;
                        entry.1 += record.last_size;
                        
                        if stats.successful_reads % 10_000 == 0 {
                            let vwap = entry.0 / entry.1 as f64;
                            println!(
                                "Analytics - Instrument {}: VWAP: {:.3}", 
                                token,
                                vwap
                            );
                        }
                    }
                }
                
                // Sample every 50ms
                thread::sleep(Duration::from_millis(50));
            }
            
            stats
        });
        
        consumer_handles.push(handle);
    }

    // Run the system and inject some error conditions
    println!("Running system for 30 seconds with normal conditions...");
    thread::sleep(Duration::from_secs(10));
    
    println!("Injecting error conditions for 10 seconds...");
    error_injection.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_secs(10));
    error_injection.store(false, Ordering::SeqCst);
    
    println!("Running system for 10 more seconds with normal conditions...");
    thread::sleep(Duration::from_secs(10));
    
    // Shutdown
    running.store(false, Ordering::SeqCst);

    // Collect and analyze results
    let mut producer_stats = vec![];
    for handle in producer_handles {
        producer_stats.push(handle.join().unwrap());
    }
    
    let mut consumer_stats = vec![];
    for handle in consumer_handles {
        consumer_stats.push(handle.join().unwrap());
    }

    // Print comprehensive statistics
    println!("\nSystem Performance Statistics");
    println!("===========================");
    
    println!("\nProducer Statistics:");
    println!("------------------");
    let total_producer_stats = producer_stats.iter().fold(
        ProducerStats {
            total_attempts: 0,
            successful_writes: 0,
            buffer_full_count: 0,
            validation_failures: 0,
            total_latency: 0,
        },
        |mut acc, stat| {
            acc.total_attempts += stat.total_attempts;
            acc.successful_writes += stat.successful_writes;
            acc.buffer_full_count += stat.buffer_full_count;
            acc.validation_failures += stat.validation_failures;
            acc.total_latency += stat.total_latency;
            acc
        }
    );
    
    println!("Total Attempts: {}", total_producer_stats.total_attempts);
    println!("Successful Writes: {}", total_producer_stats.successful_writes);
    println!("Buffer Full Count: {}", total_producer_stats.buffer_full_count);
    println!("Validation Failures: {}", total_producer_stats.validation_failures);
    println!("Average Latency: {} ns", 
        cycles_to_nanos(total_producer_stats.total_latency / total_producer_stats.successful_writes));
    
    println!("\nConsumer Statistics:");
    println!("------------------");
    let total_consumer_stats = consumer_stats.iter().fold(
        ConsumerStats {
            total_attempts: 0,
            successful_reads: 0,
            buffer_empty_count: 0,
            total_latency: 0,
            missed_updates: 0,
        },
        |mut acc, stat| {
            acc.total_attempts += stat.total_attempts;
            acc.successful_reads += stat.successful_reads;
            acc.buffer_empty_count += stat.buffer_empty_count;
            acc.total_latency += stat.total_latency;
            acc.missed_updates += stat.missed_updates;
            acc
        }
    );
    
    println!("Total Attempts: {}", total_consumer_stats.total_attempts);
    println!("Successful Reads: {}", total_consumer_stats.successful_reads);
    println!("Buffer Empty Count: {}", total_consumer_stats.buffer_empty_count);
    println!("Missed Updates: {}", total_consumer_stats.missed_updates);
    println!("Average Latency: {} ns",
        cycles_to_nanos(total_consumer_stats.total_latency / total_consumer_stats.successful_reads));

    // Verify system integrity
    assert!(total_producer_stats.successful_writes > 0, "No successful writes");
    assert!(total_consumer_stats.successful_reads > 0, "No successful reads");
    assert!(total_consumer_stats.successful_reads <= total_producer_stats.successful_writes,
        "Read more records than were written");
    assert!(total_consumer_stats.missed_updates < total_producer_stats.successful_writes / 100,
        "Too many missed updates (>1%)");
}

// Helper function from the previous test
fn cycles_to_nanos(cycles: u64) -> u64 {
    cycles * 1_000_000_000 / 3_000_000_000
} 