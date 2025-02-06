use std::sync::Arc;
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use tokio::time::sleep;
use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use url::Url;
use tokio_tungstenite::connect_async;
use futures::SinkExt;
use chrono::{Utc, TimeZone};

use crate::{
    MarketDataRecord,
    ZeroAllocRingBuffer,
    store::RedisManager,
    timeseries::TimeSeriesManager,
    rdtsc_timestamp,
    init_hardware_optimizations,
};

#[derive(Debug, Default)]
struct SystemStats {
    total_messages: AtomicU64,
    ring_buffer_writes: AtomicU64,
    redis_publishes: AtomicU64,
    timeseries_writes: AtomicU64,
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
async fn test_full_system_integration() -> Result<()> {
    println!("\nRunning Full System Integration Test");
    println!("===================================");

    // Initialize hardware optimizations
    unsafe { init_hardware_optimizations(); }

    // Create components
    let ring_buffer = Arc::new(ZeroAllocRingBuffer::<MarketDataRecord>::new(1_048_576));
    let redis = Arc::new(RedisManager::new("redis://localhost:6379")?);
    let timeseries = Arc::new(TimeSeriesManager::new()?);
    
    // Reset database schema for testing
    TimeSeriesManager::reset_database_schema(&timeseries.pool).await?;
    
    // Wait for schema initialization
    sleep(Duration::from_secs(2)).await;
    
    // Initialize statistics
    let stats = Arc::new(SystemStats::new());
    let running = Arc::new(AtomicBool::new(true));

    // Create Redis subscriber
    let mut redis_rx = redis.subscribe("market_data");

    // Test different message types and flows
    println!("Testing L1 price updates...");
    test_l1_price_updates(&ring_buffer, &redis, &timeseries, &stats).await?;
    
    println!("Testing L2 trade updates...");
    test_l2_trade_updates(&ring_buffer, &redis, &timeseries, &stats).await?;
    
    println!("Testing historical data...");
    test_historical_data(&timeseries).await?;
    
    println!("Testing high throughput...");
    test_high_throughput(&ring_buffer, &redis, &timeseries, &stats).await?;

    // Print statistics
    print_system_stats(&stats, &redis, &timeseries);

    Ok(())
}

async fn test_l1_price_updates(
    ring_buffer: &ZeroAllocRingBuffer<MarketDataRecord>,
    redis: &RedisManager,
    timeseries: &TimeSeriesManager,
    stats: &SystemStats,
) -> Result<()> {
    println!("Creating L1 price update record...");
    
    // Use current time in nanoseconds
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let record = MarketDataRecord::new(
        1001,   // token
        100.0,  // bid
        100.1,  // ask
        100,    // bid size
        100,    // ask size
        100.05, // last
        50,     // last size
        1,      // seq
        current_time,  // Use current time instead of rdtsc
        0,      // flags
    );
    println!("Created record with timestamp: {}", Utc.timestamp_opt(
        (current_time / 1_000_000_000) as i64,
        (current_time % 1_000_000_000) as u32,
    ).unwrap());
    println!("Record details: {:?}", record);
    
    // Write to ring buffer
    println!("Writing to ring buffer...");
    let start = Instant::now();
    unsafe {
        if ring_buffer.write(&record) {
            let latency = start.elapsed().as_nanos() as u64;
            stats.ring_buffer_writes.fetch_add(1, Ordering::Relaxed);
            stats.update_latency(latency);
            println!("Successfully wrote to ring buffer with latency: {} ns", latency);
        } else {
            println!("Failed to write to ring buffer!");
        }
    }
    
    // Publish to Redis
    println!("Publishing to Redis...");
    let redis_start = Instant::now();
    match redis.publish_message("market_data", &record).await {
        Ok(_) => {
            let redis_latency = redis_start.elapsed().as_nanos();
            stats.redis_publishes.fetch_add(1, Ordering::Relaxed);
            println!("Successfully published to Redis with latency: {} ns", redis_latency);
        },
        Err(e) => println!("Failed to publish to Redis: {}", e),
    }
    
    // Store in TimeSeries
    println!("Storing in TimeSeries...");
    let ts_start = Instant::now();
    match timeseries.store_record(&record).await {
        Ok(_) => {
            let ts_latency = ts_start.elapsed().as_nanos();
            stats.timeseries_writes.fetch_add(1, Ordering::Relaxed);
            println!("Successfully stored in TimeSeries with latency: {} ns", ts_latency);
        },
        Err(e) => println!("Failed to store in TimeSeries: {}", e),
    }
    
    // Verify data
    println!("Querying TimeSeries for verification...");
    let query_start = Instant::now();
    let start_time = Utc::now() - chrono::Duration::minutes(1);
    let end_time = Utc::now() + chrono::Duration::minutes(1);
    println!("Query range: {} to {}", start_time, end_time);
    
    let stored_records = timeseries.query_range(
        1001,
        start_time,
        end_time,
    ).await?;
    
    let query_latency = query_start.elapsed().as_nanos();
    println!("Query completed in {} ns", query_latency);
    println!("Retrieved {} records", stored_records.len());
    
    if stored_records.is_empty() {
        println!("WARNING: No records found in TimeSeries!");
    } else {
        println!("Found record: {:?}", stored_records[0]);
    }
    
    assert!(!stored_records.is_empty(), "No records found in TimeSeries query");
    assert_eq!(stored_records[0].token, record.token, "Token mismatch");
    assert_eq!(stored_records[0].last_price, record.last_price, "Price mismatch");
    
    println!("L1 price update test completed successfully");
    Ok(())
}

async fn test_l2_trade_updates(
    ring_buffer: &ZeroAllocRingBuffer<MarketDataRecord>,
    redis: &RedisManager,
    timeseries: &TimeSeriesManager,
    stats: &SystemStats,
) -> Result<()> {
    println!("Creating L2 trade update record...");
    
    // Use current time in nanoseconds
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let record = MarketDataRecord::new(
        1002,   // token
        101.0,  // bid
        101.1,  // ask
        200,    // bid size
        200,    // ask size
        101.05, // last
        150,    // last size
        2,      // seq
        current_time,  // Use current time instead of rdtsc
        0,      // flags
    );
    println!("Created record with timestamp: {}", Utc.timestamp_opt(
        (current_time / 1_000_000_000) as i64,
        (current_time % 1_000_000_000) as u32,
    ).unwrap());
    println!("Record details: {:?}", record);
    
    // Write to ring buffer
    let start = Instant::now();
    unsafe {
        if ring_buffer.write(&record) {
            let latency = start.elapsed().as_nanos() as u64;
            stats.ring_buffer_writes.fetch_add(1, Ordering::Relaxed);
            stats.update_latency(latency);
            println!("Successfully wrote to ring buffer with latency: {} ns", latency);
        } else {
            println!("Failed to write to ring buffer!");
        }
    }
    
    // Publish to Redis
    let redis_start = Instant::now();
    match redis.publish_message("market_data", &record).await {
        Ok(_) => {
            let redis_latency = redis_start.elapsed().as_nanos();
            stats.redis_publishes.fetch_add(1, Ordering::Relaxed);
            println!("Successfully published to Redis with latency: {} ns", redis_latency);
        },
        Err(e) => println!("Failed to publish to Redis: {}", e),
    }
    
    // Store in TimeSeries
    let ts_start = Instant::now();
    match timeseries.store_record(&record).await {
        Ok(_) => {
            let ts_latency = ts_start.elapsed().as_nanos();
            stats.timeseries_writes.fetch_add(1, Ordering::Relaxed);
            println!("Successfully stored in TimeSeries with latency: {} ns", ts_latency);
        },
        Err(e) => println!("Failed to store in TimeSeries: {}", e),
    }
    
    // Verify data
    println!("Querying TimeSeries for verification...");
    let query_start = Instant::now();
    let start_time = Utc::now() - chrono::Duration::minutes(1);
    let end_time = Utc::now() + chrono::Duration::minutes(1);
    println!("Query range: {} to {}", start_time, end_time);
    
    let stored_records = timeseries.query_range(
        1002,
        start_time,
        end_time,
    ).await?;
    
    let query_latency = query_start.elapsed().as_nanos();
    println!("Query completed in {} ns", query_latency);
    println!("Retrieved {} records", stored_records.len());
    
    if stored_records.is_empty() {
        println!("WARNING: No records found in TimeSeries!");
    } else {
        println!("Found record: {:?}", stored_records[0]);
    }
    
    assert!(!stored_records.is_empty(), "No records found in TimeSeries query");
    assert_eq!(stored_records[0].token, record.token, "Token mismatch");
    assert_eq!(stored_records[0].last_price, record.last_price, "Price mismatch");
    
    println!("L2 trade update test completed successfully");
    Ok(())
}

async fn test_historical_data(timeseries: &TimeSeriesManager) -> Result<()> {
    println!("Creating historical data record...");
    
    // Use current time in nanoseconds
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let record = MarketDataRecord::new(
        1003,   // token
        102.0,  // bid
        102.1,  // ask
        300,    // bid size
        300,    // ask size
        102.05, // last
        250,    // last size
        3,      // seq
        current_time,  // Use current time instead of rdtsc
        0,      // flags
    );
    println!("Created record with timestamp: {}", Utc.timestamp_opt(
        (current_time / 1_000_000_000) as i64,
        (current_time % 1_000_000_000) as u32,
    ).unwrap());
    println!("Record details: {:?}", record);
    
    // Store record
    println!("Storing record in TimeSeries...");
    let store_start = Instant::now();
    match timeseries.store_record(&record).await {
        Ok(_) => {
            let store_latency = store_start.elapsed().as_nanos();
            println!("Successfully stored record with latency: {} ns", store_latency);
        },
        Err(e) => println!("Failed to store record: {}", e),
    }
    
    // Query back
    println!("Querying TimeSeries for verification...");
    let query_start = Instant::now();
    let start_time = Utc::now() - chrono::Duration::minutes(1);
    let end_time = Utc::now() + chrono::Duration::minutes(1);
    println!("Query range: {} to {}", start_time, end_time);
    
    let records = timeseries.query_range(1003, start_time, end_time).await?;
    
    let query_latency = query_start.elapsed().as_nanos();
    println!("Query completed in {} ns", query_latency);
    println!("Retrieved {} records", records.len());
    
    if records.is_empty() {
        println!("WARNING: No records found in TimeSeries!");
    } else {
        println!("Found record: {:?}", records[0]);
    }
    
    assert!(!records.is_empty(), "No records found in TimeSeries query");
    assert_eq!(records[0].token, record.token, "Token mismatch");
    assert_eq!(records[0].last_price, record.last_price, "Price mismatch");
    
    println!("Historical data test completed successfully");
    Ok(())
}

async fn test_high_throughput(
    ring_buffer: &ZeroAllocRingBuffer<MarketDataRecord>,
    redis: &RedisManager,
    timeseries: &TimeSeriesManager,
    stats: &SystemStats,
) -> Result<()> {
    const NUM_MESSAGES: u64 = 1_000;
    const BATCH_SIZE: usize = 20;
    let mut sequence = 0u64;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    
    while sequence < NUM_MESSAGES {
        // Create batch of messages
        batch.clear();
        for _ in 0..BATCH_SIZE.min((NUM_MESSAGES - sequence) as usize) {
            let timestamp = unsafe { rdtsc_timestamp() };
            batch.push(MarketDataRecord::new(
                2001 + sequence,
                100.0 + (sequence % 100) as f64,
                100.1 + (sequence % 100) as f64,
                100,
                100,
                100.05,
                50,
                sequence,
                timestamp,
                0,
            ));
            sequence += 1;
        }
        
        // Process batch
        let start = Instant::now();
        for record in batch.iter() {
            // Write to ring buffer
            unsafe {
                if ring_buffer.write(record) {
                    stats.ring_buffer_writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.buffer_full_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            
            // Publish to Redis
            redis.publish_message("market_data", record).await?;
            stats.redis_publishes.fetch_add(1, Ordering::Relaxed);
            
            // Store in TimeSeries
            timeseries.store_record(record).await?;
            stats.timeseries_writes.fetch_add(1, Ordering::Relaxed);
        }
        
        let latency = start.elapsed().as_nanos() as u64;
        stats.update_latency(latency / batch.len() as u64);
        
        // Print progress
        if sequence % 100 == 0 {
            println!("Processed {} messages", sequence);
        }
        
        // Small delay to prevent overwhelming the system
        sleep(Duration::from_millis(10)).await;
    }
    
    Ok(())
}

fn print_system_stats(
    stats: &SystemStats,
    redis: &RedisManager,
    timeseries: &TimeSeriesManager,
) {
    let total_messages = stats.total_messages.load(Ordering::Relaxed);
    let ring_buffer_writes = stats.ring_buffer_writes.load(Ordering::Relaxed);
    let redis_publishes = stats.redis_publishes.load(Ordering::Relaxed);
    let timeseries_writes = stats.timeseries_writes.load(Ordering::Relaxed);
    let buffer_full_count = stats.buffer_full_count.load(Ordering::Relaxed);
    let total_latency_ns = stats.total_latency_ns.load(Ordering::Relaxed);
    let min_latency_ns = stats.min_latency_ns.load(Ordering::Relaxed);
    let max_latency_ns = stats.max_latency_ns.load(Ordering::Relaxed);

    println!("\nSystem Performance Statistics:");
    println!("============================");
    println!("Message Processing:");
    println!("  Total Messages: {}", total_messages);
    println!("  Ring Buffer Writes: {} ({:.2} million/sec)",
        ring_buffer_writes,
        ring_buffer_writes as f64 / 5.0 / 1_000_000.0
    );
    println!("  Redis Publishes: {} ({:.2} million/sec)",
        redis_publishes,
        redis_publishes as f64 / 5.0 / 1_000_000.0
    );
    println!("  TimeSeries Writes: {} ({:.2} million/sec)",
        timeseries_writes,
        timeseries_writes as f64 / 5.0 / 1_000_000.0
    );
    println!("  Buffer Full Count: {} ({:.2}%)",
        buffer_full_count,
        (buffer_full_count as f64 / ring_buffer_writes as f64) * 100.0
    );

    println!("\nLatency Statistics:");
    println!("  Minimum Latency: {:.2} ns", min_latency_ns);
    println!("  Maximum Latency: {:.2} ns", max_latency_ns);
    println!("  Average Latency: {:.2} ns",
        total_latency_ns as f64 / (ring_buffer_writes + redis_publishes + timeseries_writes) as f64
    );

    println!("\nRedis Statistics:");
    let redis_stats = redis.get_stats();
    println!("  Messages Published: {}", redis_stats.messages_published.load(Ordering::Relaxed));
    println!("  Active Subscribers: {}", redis_stats.subscribers.load(Ordering::Relaxed));
    println!("  Average Publish Latency: {:.2} ns",
        redis_stats.publish_latency_ns.load(Ordering::Relaxed) as f64 / 
        redis_stats.messages_published.load(Ordering::Relaxed) as f64
    );

    println!("\nTimeSeries Statistics:");
    let ts_stats = timeseries.get_stats();
    println!("  Records Stored: {}", ts_stats.records_stored.load(Ordering::Relaxed));
    println!("  Bytes Written: {}", ts_stats.bytes_written.load(Ordering::Relaxed));
    println!("  Compression Ratio: {:.2}x",
        ts_stats.compression_ratio.load(Ordering::Relaxed) as f64 / 1000.0
    );
    println!("  Average Write Latency: {:.2} ns",
        ts_stats.write_latency_ns.load(Ordering::Relaxed) as f64 / 
        ts_stats.records_stored.load(Ordering::Relaxed) as f64
    );
    println!("  Average Query Latency: {:.2} ns",
        ts_stats.query_latency_ns.load(Ordering::Relaxed) as f64 / 
        ts_stats.records_stored.load(Ordering::Relaxed) as f64
    );
} 