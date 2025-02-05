use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::core::{
    record::MarketDataRecord,
    config::{InstrumentBufferConfig, BufferType},
};
use crate::memory::instrument_buffer::InstrumentBufferManager;
use crate::tests::rdtsc_serialized;

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
    
    let manager = Arc::new(manager);
    let mut producer_handles: Vec<thread::JoinHandle<ProducerStats>> = vec![];
    let mut consumer_handles: Vec<thread::JoinHandle<ConsumerStats>> = vec![];
    
    // Create producer threads for different market data sources
    {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let instruments = primary_instruments.clone();
        producer_handles.push(thread::spawn(move || {
            let mut stats = ProducerStats {
                total_attempts: 0,
                successful_writes: 0,
                buffer_full_count: 0,
                validation_failures: 0,
                total_latency: 0,
            };
            
            while running.load(Ordering::Relaxed) {
                for &token in &instruments {
                    stats.total_attempts += 1;
                    let record = MarketDataRecord::new(
                        token,
                        100.0 + (stats.total_attempts % 100) as f64,
                        100.5 + (stats.total_attempts % 100) as f64,
                        100,
                        100,
                        100.25,
                        50,
                        unsafe { rdtsc_serialized() },
                        stats.total_attempts,
                        1,
                    );
                    
                    unsafe {
                        if manager.write(token, &record, BufferType::L1Price) {
                            stats.successful_writes += 1;
                        } else {
                            stats.buffer_full_count += 1;
                        }
                    }
                    
                    thread::sleep(Duration::from_micros(100)); // Simulate 100μs between updates
                }
            }
            stats
        }));
    }
    
    // Create consumer threads
    {
        let manager = Arc::clone(&manager);
        let running = Arc::clone(&running);
        let instruments = primary_instruments.clone();
        consumer_handles.push(thread::spawn(move || {
            let mut stats = ConsumerStats {
                total_attempts: 0,
                successful_reads: 0,
                buffer_empty_count: 0,
                total_latency: 0,
                missed_updates: 0,
            };
            
            while running.load(Ordering::Relaxed) {
                for &token in &instruments {
                    stats.total_attempts += 1;
                    unsafe {
                        if let Some(_record) = manager.read(token, BufferType::L1Price) {
                            stats.successful_reads += 1;
                        } else {
                            stats.buffer_empty_count += 1;
                        }
                    }
                    thread::sleep(Duration::from_micros(50)); // Read twice as fast as writes
                }
            }
            stats
        }));
    }
    
    // Let the test run for 5 seconds
    thread::sleep(Duration::from_secs(5));
    running.store(false, Ordering::Relaxed);
    
    // Collect results
    let producer_stats: Vec<ProducerStats> = producer_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
        
    let consumer_stats: Vec<ConsumerStats> = consumer_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    
    // Print results
    for (i, stats) in producer_stats.iter().enumerate() {
        println!("\nProducer {} Statistics:", i);
        println!("Total attempts: {}", stats.total_attempts);
        println!("Successful writes: {}", stats.successful_writes);
        println!("Buffer full count: {}", stats.buffer_full_count);
        println!("Success rate: {:.2}%", 
            (stats.successful_writes as f64 / stats.total_attempts as f64) * 100.0);
    }
    
    for (i, stats) in consumer_stats.iter().enumerate() {
        println!("\nConsumer {} Statistics:", i);
        println!("Total attempts: {}", stats.total_attempts);
        println!("Successful reads: {}", stats.successful_reads);
        println!("Buffer empty count: {}", stats.buffer_empty_count);
        println!("Success rate: {:.2}%", 
            (stats.successful_reads as f64 / stats.total_attempts as f64) * 100.0);
    }
}

#[derive(Debug)]
struct ProducerStats {
    total_attempts: u64,
    successful_writes: u64,
    buffer_full_count: u64,
    validation_failures: u64,
    total_latency: u64,
}

#[derive(Debug)]
struct ConsumerStats {
    total_attempts: u64,
    successful_reads: u64,
    buffer_empty_count: u64,
    total_latency: u64,
    missed_updates: u64,
} 