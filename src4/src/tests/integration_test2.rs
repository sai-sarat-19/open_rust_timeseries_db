use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use tokio::time::sleep;
use anyhow::Result;
use futures::SinkExt;
use chrono::Utc;
use std::thread;

use crate::{
    FeedMessage, FeedSource, MessageType,
    GlobalMarketData, GlobalConfig,
    TimeSeriesManager, TimeSeriesConfig,
    RedisManager, InstrumentBufferConfig,
};

// Ultra-low-latency record for in-memory storage
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
struct UltraLowLatencyRecord {
    token: u64,
    bid_price: f64,
    ask_price: f64,
    bid_size: u32,
    ask_size: u32,
    last_price: f64,
    last_size: u32,
    sequence_num: u64,
    timestamp: u64,
    flags: u8,
    _padding: [u8; 7],  // Pad to 64-byte cache line
}

// Zero-allocation ring buffer for ultra-low-latency storage
#[repr(align(64))]
struct ZeroAllocRingBuffer {
    data: Box<[UltraLowLatencyRecord]>,
    capacity: usize,
    write_pos: AtomicU64,
    read_pos: AtomicU64,
    _pad: [u8; 40],  // Ensure no false sharing
}

impl ZeroAllocRingBuffer {
    fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        unsafe { data.set_len(capacity); }
        Self {
            data: data.into_boxed_slice(),
            capacity,
            write_pos: AtomicU64::new(0),
            read_pos: AtomicU64::new(0),
            _pad: [0; 40],
        }
    }

    #[inline(always)]
    unsafe fn write(&self, record: &UltraLowLatencyRecord) -> bool {
        let write_pos = self.write_pos.load(Ordering::Relaxed) as usize;
        let next_write = (write_pos + 1) % self.capacity;
        
        if next_write == (self.read_pos.load(Ordering::Acquire) as usize) {
            return false;  // Buffer full
        }

        // Zero-copy write
        std::ptr::copy_nonoverlapping(
            record as *const UltraLowLatencyRecord,
            self.data.as_ptr().add(write_pos) as *mut UltraLowLatencyRecord,
            1
        );
        
        fence(Ordering::Release);
        self.write_pos.store(next_write as u64, Ordering::Release);
        true
    }

    #[inline(always)]
    unsafe fn read(&self) -> Option<UltraLowLatencyRecord> {
        let read_pos = self.read_pos.load(Ordering::Relaxed) as usize;
        if read_pos == (self.write_pos.load(Ordering::Acquire) as usize) {
            return None;  // Buffer empty
        }

        let record = std::ptr::read(self.data.as_ptr().add(read_pos));
        let next_read = (read_pos + 1) % self.capacity;
        self.read_pos.store(next_read as u64, Ordering::Release);
        Some(record)
    }
}

// Statistics for monitoring system performance
#[derive(Debug, Default)]
struct SystemStats {
    total_messages: AtomicU64,
    memory_writes: AtomicU64,
    memory_reads: AtomicU64,
    buffer_full_count: AtomicU64,
    redis_publish_count: AtomicU64,
    timeseries_write_count: AtomicU64,
    memory_latency_ns: AtomicU64,
    redis_latency_ns: AtomicU64,
    timeseries_latency_ns: AtomicU64,
}

#[tokio::test]
async fn test_integrated_buffer_system() -> Result<()> {
    println!("\nRunning Ultra-Low-Latency Integrated System Test");
    println!("==============================================");

    // Initialize components
    println!("Initializing system components...");
    
    // Create ultra-low-latency ring buffer
    let ring_buffer = Arc::new(ZeroAllocRingBuffer::new(1_048_576));  // 1M capacity
    
    // Initialize persistent storage components
    let time_series = TimeSeriesManager::new()?;
    println!("Resetting database schema...");
    TimeSeriesManager::reset_database_schema(&time_series.pool).await?;
    
    let redis = Arc::new(RedisManager::new("redis://localhost:6379")?);
    let market_data = Arc::new(GlobalMarketData::new_with_redis(GlobalConfig {
        num_instruments: 10_000,
        cache_size_mb: 1024,
        num_threads: num_cpus::get(),
        buffer_config: InstrumentBufferConfig {
            l1_buffer_size: 1_048_576,
            l2_buffer_size: 524_288,
            ref_buffer_size: 65_536,
        },
    }, Arc::clone(&redis))?);

    // Create Redis subscriber
    let mut redis_rx = redis.subscribe("market_data");
    
    // Initialize statistics
    let stats = Arc::new(SystemStats::default());
    let running = Arc::new(AtomicBool::new(true));

    // Spawn ultra-low-latency producer thread
    let producer_stats = Arc::clone(&stats);
    let producer_buffer = Arc::clone(&ring_buffer);
    let producer_running = Arc::clone(&running);
    
    let producer_handle = thread::spawn(move || {
        let mut sequence = 0u64;
        while producer_running.load(Ordering::Relaxed) {
            let record = UltraLowLatencyRecord {
                token: 1001,
                bid_price: 100.0 + (sequence % 100) as f64,
                ask_price: 100.1 + (sequence % 100) as f64,
                bid_size: 100,
                ask_size: 100,
                last_price: 100.05,
                last_size: 50,
                sequence_num: sequence,
                timestamp: unsafe { rdtsc_serialized() },
                flags: 0,
                _padding: [0; 7],
            };

            let start = Instant::now();
            unsafe {
                if producer_buffer.write(&record) {
                    producer_stats.memory_writes.fetch_add(1, Ordering::Relaxed);
                    producer_stats.memory_latency_ns.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                } else {
                    producer_stats.buffer_full_count.fetch_add(1, Ordering::Relaxed);
                }
            }

            sequence += 1;
            thread::sleep(Duration::from_nanos(100));  // Simulate 100ns between updates
        }
    });

    // Spawn consumer thread for persistent storage
    let consumer_stats = Arc::clone(&stats);
    let consumer_buffer = Arc::clone(&ring_buffer);
    let consumer_market_data = Arc::clone(&market_data);
    let consumer_redis = Arc::clone(&redis);
    let consumer_time_series = time_series.clone();
    let consumer_running = Arc::clone(&running);

    let consumer_handle = tokio::spawn(async move {
        while consumer_running.load(Ordering::Relaxed) {
            unsafe {
                if let Some(record) = consumer_buffer.read() {
                    consumer_stats.memory_reads.fetch_add(1, Ordering::Relaxed);

                    // Convert to FeedMessage
                    let msg = FeedMessage::new(
                        record.token,
                        record.bid_price,
                        record.ask_price,
                        record.bid_size,
                        record.ask_size,
                        record.last_price,
                        record.last_size,
                        record.sequence_num,
                        FeedSource::PrimaryExchange,
                        MessageType::L1Update,
                    );

                    // Store in persistent storage
                    let start = Instant::now();
                    if let Ok(()) = consumer_market_data.process_feed_message(msg.clone()).await {
                        consumer_stats.timeseries_latency_ns.fetch_add(
                            start.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        consumer_stats.timeseries_write_count.fetch_add(1, Ordering::Relaxed);
                    }

                    let start = Instant::now();
                    if let Ok(()) = consumer_redis.publish_message("market_data", &msg).await {
                        consumer_stats.redis_latency_ns.fetch_add(
                            start.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        consumer_stats.redis_publish_count.fetch_add(1, Ordering::Relaxed);
                    }

                    consumer_stats.total_messages.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_nanos(50)).await;  // Read twice as fast as writes
        }
    });

    // Run test for 5 seconds
    println!("Running test for 5 seconds...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for threads to complete
    producer_handle.join().unwrap();
    consumer_handle.await.unwrap();

    // Print statistics
    print_system_stats(&stats, &market_data, &redis, &time_series);

    Ok(())
}

#[inline(always)]
unsafe fn rdtsc_serialized() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_mm_lfence, _mm_mfence, _rdtsc};
        _mm_mfence();
        _mm_lfence();
        let tsc = _rdtsc();
        _mm_lfence();
        tsc
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

fn print_system_stats(
    stats: &SystemStats,
    market_data: &GlobalMarketData,
    redis: &RedisManager,
    time_series: &TimeSeriesManager,
) {
    let total_messages = stats.total_messages.load(Ordering::Relaxed);
    let memory_writes = stats.memory_writes.load(Ordering::Relaxed);
    let memory_reads = stats.memory_reads.load(Ordering::Relaxed);
    let buffer_full_count = stats.buffer_full_count.load(Ordering::Relaxed);
    let redis_publish_count = stats.redis_publish_count.load(Ordering::Relaxed);
    let timeseries_write_count = stats.timeseries_write_count.load(Ordering::Relaxed);

    println!("\nUltra-Low-Latency Performance Statistics:");
    println!("======================================");
    println!("Memory Ring Buffer:");
    println!("  Total Writes: {} ({:.2} million/sec)",
        memory_writes,
        memory_writes as f64 / 5.0 / 1_000_000.0
    );
    println!("  Total Reads: {} ({:.2} million/sec)",
        memory_reads,
        memory_reads as f64 / 5.0 / 1_000_000.0
    );
    println!("  Buffer Full Count: {} ({:.2}%)",
        buffer_full_count,
        (buffer_full_count as f64 / memory_writes as f64) * 100.0
    );
    println!("  Average Memory Latency: {:.2} ns",
        stats.memory_latency_ns.load(Ordering::Relaxed) as f64 / memory_writes as f64
    );

    println!("\nPersistent Storage Performance:");
    println!("Redis:");
    println!("  Messages Published: {} ({:.2}%)",
        redis_publish_count,
        (redis_publish_count as f64 / total_messages as f64) * 100.0
    );
    println!("  Average Publish Latency: {:.2} μs",
        stats.redis_latency_ns.load(Ordering::Relaxed) as f64 / redis_publish_count as f64 / 1000.0
    );

    println!("\nTimeSeries DB:");
    println!("  Records Stored: {} ({:.2}%)",
        timeseries_write_count,
        (timeseries_write_count as f64 / total_messages as f64) * 100.0
    );
    println!("  Average Write Latency: {:.2} μs",
        stats.timeseries_latency_ns.load(Ordering::Relaxed) as f64 / timeseries_write_count as f64 / 1000.0
    );

    // Component-specific stats
    let md_stats = market_data.get_stats();
    let redis_stats = redis.get_stats();
    let ts_stats = time_series.get_stats();

    println!("\nComponent Statistics:");
    println!("Market Data Store:");
    println!("  Total Updates: {}", md_stats.total_updates);
    println!("  Subscriber Count: {}", md_stats.subscriber_count);

    println!("\nRedis Stats:");
    println!("  Total Published: {}", redis_stats.messages_published);
    println!("  Active Subscribers: {}", redis_stats.subscribers);

    println!("\nTimeSeries Stats:");
    println!("  Total Records: {}", ts_stats.records_stored);
    println!("  Compression Ratio: {:.2}", ts_stats.compression_ratio);
} 