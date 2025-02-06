use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::time::sleep;
use anyhow::Result;
use futures::SinkExt;
use chrono::Utc;
use std::thread;

use crate::{
    UltraLowLatencyRecord,
    MarketDataRecord,
    ZeroAllocRingBuffer,
    rdtsc_timestamp,
    init_hardware_optimizations,
};

// Statistics for monitoring system performance
#[derive(Debug, Default)]
struct SystemStats {
    total_messages: AtomicU64,
    memory_writes: AtomicU64,
    memory_reads: AtomicU64,
    buffer_full_count: AtomicU64,
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
}

impl SystemStats {
    fn new() -> Self {
        Self {
            min_latency_ns: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    fn update_latency(&self, latency_ns: u64) {
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update min latency
        let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.min_latency_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max latency
        let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }
}

#[tokio::test]
async fn test_ultra_low_latency_system() -> Result<()> {
    println!("\nRunning Ultra-Low-Latency System Test");
    println!("=====================================");

    // Initialize hardware optimizations
    unsafe { init_hardware_optimizations(); }

    // Create ring buffer with 1M capacity
    let ring_buffer = Arc::new(ZeroAllocRingBuffer::<MarketDataRecord>::new(1_048_576));
    
    // Initialize statistics
    let stats = Arc::new(SystemStats::new());
    let running = Arc::new(AtomicBool::new(true));

    // Spawn producer thread
    let producer_stats = Arc::clone(&stats);
    let producer_buffer = Arc::clone(&ring_buffer);
    let producer_running = Arc::clone(&running);
    
    let producer_handle = thread::spawn(move || {
        let mut sequence = 0u64;
        
        while producer_running.load(Ordering::Relaxed) {
            let timestamp = unsafe { rdtsc_timestamp() };
            
            let record = MarketDataRecord::new(
                1001,
                100.0 + (sequence % 100) as f64,
                100.1 + (sequence % 100) as f64,
                100,
                100,
                100.05,
                50,
                sequence,
                timestamp,
                0,
            );

            let start = Instant::now();
            unsafe {
                if producer_buffer.write(&record) {
                    let latency = start.elapsed().as_nanos() as u64;
                    producer_stats.memory_writes.fetch_add(1, Ordering::Relaxed);
                    producer_stats.update_latency(latency);
                } else {
                    producer_stats.buffer_full_count.fetch_add(1, Ordering::Relaxed);
                }
            }

            sequence += 1;
            
            // Ultra-low latency sleep using spin wait
            let start = Instant::now();
            while start.elapsed().as_nanos() < 100 {
                std::hint::spin_loop();
            }
        }
    });

    // Spawn consumer thread
    let consumer_stats = Arc::clone(&stats);
    let consumer_buffer = Arc::clone(&ring_buffer);
    let consumer_running = Arc::clone(&running);

    let consumer_handle = thread::spawn(move || {
        while consumer_running.load(Ordering::Relaxed) {
            let start = Instant::now();
            
            unsafe {
                if let Some(record) = consumer_buffer.read() {
                    let latency = start.elapsed().as_nanos() as u64;
                    consumer_stats.memory_reads.fetch_add(1, Ordering::Relaxed);
                    consumer_stats.update_latency(latency);
                    consumer_stats.total_messages.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Ultra-low latency sleep using spin wait
            let start = Instant::now();
            while start.elapsed().as_nanos() < 50 {
                std::hint::spin_loop();
            }
        }
    });

    // Run test for 5 seconds
    println!("Running test for 5 seconds...");
    sleep(Duration::from_secs(5)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for threads to complete
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();

    // Print statistics
    print_system_stats(&stats);

    Ok(())
}

fn print_system_stats(stats: &SystemStats) {
    let total_messages = stats.total_messages.load(Ordering::Relaxed);
    let memory_writes = stats.memory_writes.load(Ordering::Relaxed);
    let memory_reads = stats.memory_reads.load(Ordering::Relaxed);
    let buffer_full_count = stats.buffer_full_count.load(Ordering::Relaxed);
    let total_latency_ns = stats.total_latency_ns.load(Ordering::Relaxed);
    let min_latency_ns = stats.min_latency_ns.load(Ordering::Relaxed);
    let max_latency_ns = stats.max_latency_ns.load(Ordering::Relaxed);

    println!("\nUltra-Low-Latency Performance Statistics:");
    println!("======================================");
    println!("Memory Ring Buffer:");
    println!("  Total Messages: {}", total_messages);
    println!("  Write Operations: {} ({:.2} million/sec)",
        memory_writes,
        memory_writes as f64 / 5.0 / 1_000_000.0
    );
    println!("  Read Operations: {} ({:.2} million/sec)",
        memory_reads,
        memory_reads as f64 / 5.0 / 1_000_000.0
    );
    println!("  Buffer Full Count: {} ({:.2}%)",
        buffer_full_count,
        (buffer_full_count as f64 / memory_writes as f64) * 100.0
    );

    println!("\nLatency Statistics:");
    println!("  Minimum Latency: {:.2} ns", min_latency_ns);
    println!("  Maximum Latency: {:.2} ns", max_latency_ns);
    println!("  Average Latency: {:.2} ns",
        total_latency_ns as f64 / (memory_writes + memory_reads) as f64
    );
} 