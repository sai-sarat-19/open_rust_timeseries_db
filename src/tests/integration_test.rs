use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;
use std::borrow::Cow;
use std::hint::black_box;
use std::sync::atomic::AtomicBool;

use crate::storage::table::{Table, TableConfig, FieldConfig};

// Constants for performance tuning
const RING_BUFFER_SIZE: usize = 16384;  // 16K entries per field
const BATCH_SIZE: usize = 256;          // Optimal cache line usage
const CACHE_LINE_SIZE: usize = 64;      // Common CPU cache line size
const MAX_RETRIES: usize = 1000;

// Align data to cache line boundaries to prevent false sharing
#[repr(align(64))]
struct PreAllocatedRecord {
    symbol_id: [u8; 4],    // Fixed-size arrays instead of Vec
    price: [u8; 8],
    quantity: [u8; 4],
    timestamp: [u8; 8],
    exchange_id: [u8; 1],
    _padding: [u8; 39],    // Pad to cache line size
}

impl PreAllocatedRecord {
    fn new() -> Self {
        Self {
            symbol_id: [0; 4],
            price: [0; 8],
            quantity: [0; 4],
            timestamp: [0; 8],
            exchange_id: [0; 1],
            _padding: [0; 39],
        }
    }

    // Zero-copy conversion to HashMap using references
    fn to_hashmap(&self) -> HashMap<&'static str, Box<[u8]>> {
        let mut map = HashMap::with_capacity(5);
        // Convert fixed arrays to boxed slices
        map.insert("symbol_id", Vec::from(self.symbol_id.as_slice()).into_boxed_slice());
        map.insert("price", Vec::from(self.price.as_slice()).into_boxed_slice());
        map.insert("quantity", Vec::from(self.quantity.as_slice()).into_boxed_slice());
        map.insert("timestamp", Vec::from(self.timestamp.as_slice()).into_boxed_slice());
        map.insert("exchange_id", Vec::from(self.exchange_id.as_slice()).into_boxed_slice());
        map
    }
}

// Cache-aligned performance stats
#[repr(align(64))]
struct PerformanceStats {
    write_latencies: Mutex<Vec<u64>>,
    read_latencies: Mutex<Vec<u64>>,
    dropped_messages: AtomicUsize,
    total_messages: AtomicUsize,
    max_latency: AtomicU64,
    _padding: [u8; CACHE_LINE_SIZE - 40], // Pad to cache line
}

impl PerformanceStats {
    fn new(capacity: usize) -> Self {
        Self {
            write_latencies: Mutex::new(Vec::with_capacity(capacity)),
            read_latencies: Mutex::new(Vec::with_capacity(capacity)),
            dropped_messages: AtomicUsize::new(0),
            total_messages: AtomicUsize::new(0),
            max_latency: AtomicU64::new(0),
            _padding: [0; CACHE_LINE_SIZE - 40],
        }
    }

    #[inline(always)]
    fn update_max_latency(&self, latency: u64) {
        let mut current = self.max_latency.load(Ordering::Relaxed);
        while latency > current {
            match self.max_latency.compare_exchange_weak(
                current,
                latency,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    #[inline(always)]
    fn add_read_latency(&self, latency: u64) {
        if let Ok(mut latencies) = self.read_latencies.lock() {
            latencies.push(latency);
        }
    }

    #[inline(always)]
    fn add_write_latency(&self, latency: u64) {
        if let Ok(mut latencies) = self.write_latencies.lock() {
            latencies.push(latency);
        }
    }

    fn get_stats(&self) -> (Option<f64>, Option<f64>, u64) {
        let avg_write = self.write_latencies.lock().ok().map(|latencies| {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        });
        
        let avg_read = self.read_latencies.lock().ok().map(|latencies| {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        });

        let max = self.max_latency.load(Ordering::Relaxed);
        (avg_write, avg_read, max)
    }
}

/// This test demonstrates the complete functionality of our low-latency time series database
#[test]
fn test_full_market_data_system() {
    // Setup with static field names for zero allocation
    let mut fields = HashMap::new();
    let field_configs = [
        ("symbol_id", 4),
        ("price", 8),
        ("quantity", 4),
        ("timestamp", 8),
        ("exchange_id", 1),
    ];

    for &(name, size) in field_configs.iter() {
        fields.insert(name.into(), FieldConfig {
            field_size_bytes: size,
            ring_capacity: RING_BUFFER_SIZE,
        });
    }

    let table_config = TableConfig { fields };
    let table = Arc::new(Table::new("market_data".into(), table_config));
    let stats = Arc::new(PerformanceStats::new(RING_BUFFER_SIZE));
    let start_time = Instant::now();

    const PRODUCER_COUNT: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = 10_000;
    let mut producers = Vec::with_capacity(PRODUCER_COUNT);
    
    // Producer threads
    for p_id in 0..PRODUCER_COUNT {
        let table = Arc::clone(&table);
        let stats = Arc::clone(&stats);
        
        let handle = thread::spawn(move || {
            let mut record = PreAllocatedRecord::new();
            let mut batch_count = 0;
            let mut retry_count = 0;
            
            for i in 0..MESSAGES_PER_PRODUCER {
                // Direct memory writes without intermediate allocations
                record.symbol_id.copy_from_slice(&((100 + p_id) as u32).to_le_bytes());
                record.price.copy_from_slice(&((1000.0 + (i as f64) * 0.01) as f64).to_le_bytes());
                record.quantity.copy_from_slice(&(100 + (i % 100) as u32).to_le_bytes());
                record.exchange_id[0] = p_id as u8;
                
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                record.timestamp.copy_from_slice(&timestamp.to_le_bytes());

                let write_start = Instant::now();
                let mut success = false;
                
                while !success && retry_count < MAX_RETRIES {
                    if table.write_record(record.to_hashmap()) {
                        let latency = write_start.elapsed().as_nanos() as u64;
                        stats.update_max_latency(latency);
                        stats.add_write_latency(latency);
                        stats.total_messages.fetch_add(1, Ordering::Relaxed);
                        success = true;
                        retry_count = 0;
                    } else {
                        retry_count += 1;
                        if retry_count % 10 == 0 {
                            thread::yield_now();
                        }
                    }
                }

                if !success {
                    stats.dropped_messages.fetch_add(1, Ordering::Relaxed);
                }

                batch_count += 1;
                if batch_count >= BATCH_SIZE {
                    thread::yield_now();
                    batch_count = 0;
                }
            }
        });
        producers.push(handle);
    }

    // Consumer threads
    const CONSUMER_COUNT: usize = 3;
    let mut consumers = Vec::with_capacity(CONSUMER_COUNT);
    
    for c_id in 0..CONSUMER_COUNT {
        let table = Arc::clone(&table);
        let stats = Arc::clone(&stats);
        
        let handle = thread::spawn(move || {
            let mut processed_count = 0;
            let mut batch_buffer = Vec::with_capacity(BATCH_SIZE);
            let target_messages = MESSAGES_PER_PRODUCER * PRODUCER_COUNT / CONSUMER_COUNT;
            
            while processed_count < target_messages {
                batch_buffer.clear();
                let read_start = Instant::now();
                
                // Batch reading for better cache utilization
                for _ in 0..BATCH_SIZE {
                    if let Some(record) = table.read_one_record() {
                        batch_buffer.push(record);
                    } else {
                        break;
                    }
                }

                if !batch_buffer.is_empty() {
                    let latency = read_start.elapsed().as_nanos() as u64;
                    stats.update_max_latency(latency);
                    stats.add_read_latency(latency);
                    
                    for record in &batch_buffer {
                        match c_id {
                            0 => {
                                // Zero-copy price tracking
                                if let (Some(price_bytes), Some(qty_bytes)) = (
                                    record.get("price"),
                                    record.get("quantity")
                                ) {
                                    if price_bytes.len() >= 8 && qty_bytes.len() >= 4 {
                                        let price = f64::from_le_bytes(price_bytes[..8].try_into().unwrap());
                                        let qty = u32::from_le_bytes(qty_bytes[..4].try_into().unwrap());
                                        if processed_count % 1000 == 0 {
                                            println!("Consumer {}: VWAP update - Price: {}, Qty: {}", 
                                                   c_id, price, qty);
                                        }
                                    }
                                }
                            },
                            1 => {
                                // Zero-copy latency analysis
                                if let Some(ts_bytes) = record.get("timestamp") {
                                    if ts_bytes.len() >= 8 {
                                        let msg_ts = u64::from_le_bytes(ts_bytes[..8].try_into().unwrap());
                                        let current = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_nanos() as u64;
                                        if current > msg_ts {
                                            stats.add_read_latency(current - msg_ts);
                                        }
                                    }
                                }
                            },
                            _ => {
                                if processed_count % 1000 == 0 {
                                    println!("Consumer {}: Processed {} messages", 
                                           c_id, processed_count);
                                }
                            }
                        }
                        processed_count += 1;
                    }
                } else {
                    if processed_count < target_messages / 2 {
                        thread::yield_now();
                    } else {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
            }
            
            println!("Consumer {} finished processing {} messages", c_id, processed_count);
        });
        consumers.push(handle);
    }

    // Wait for completion
    for p in producers {
        p.join().unwrap();
    }
    for c in consumers {
        c.join().unwrap();
    }

    // Performance analysis
    let total_time = start_time.elapsed();
    let total_messages = stats.total_messages.load(Ordering::Relaxed);
    let dropped_messages = stats.dropped_messages.load(Ordering::Relaxed);
    let messages_per_second = total_messages as f64 / total_time.as_secs_f64();
    let (avg_write_latency, avg_read_latency, max_latency) = stats.get_stats();

    println!("\nSystem Performance Summary:");
    println!("-------------------------");
    println!("Total Runtime: {:?}", total_time);
    println!("Total Messages: {}", total_messages);
    println!("Dropped Messages: {}", dropped_messages);
    println!("Messages/second: {:.2}", messages_per_second);
    println!("Average Write Latency: {:.2}ns", avg_write_latency.unwrap_or(0.0));
    println!("Average Read Latency: {:.2}ns", avg_read_latency.unwrap_or(0.0));
    println!("Max Latency: {}ns", max_latency);
    println!("Current table size: {}", table.record_count.load(Ordering::Relaxed));
}

#[cfg(test)]
mod latency_tests {
    use super::*;
    use std::time::{Duration, Instant};
    use std::hint::black_box;
    use std::sync::atomic::AtomicBool;

    // Constants for latency testing
    const WARMUP_ITERATIONS: usize = 1000;
    const TEST_ITERATIONS: usize = 100_000;
    const PERCENTILES: &[f64] = &[50.0, 90.0, 99.0, 99.9, 99.99];

    #[derive(Default)]
    struct LatencyMetrics {
        min_ns: u64,
        max_ns: u64,
        total_ns: u64,
        samples: Vec<u64>,
    }

    impl LatencyMetrics {
        fn new() -> Self {
            Self {
                min_ns: u64::MAX,
                max_ns: 0,
                total_ns: 0,
                samples: Vec::with_capacity(TEST_ITERATIONS),
            }
        }

        fn update(&mut self, latency: u64) {
            self.min_ns = self.min_ns.min(latency);
            self.max_ns = self.max_ns.max(latency);
            self.total_ns += latency;
            self.samples.push(latency);
        }

        fn percentile(&self, p: f64) -> u64 {
            let mut sorted = self.samples.clone();
            sorted.sort_unstable();
            let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
            sorted[index]
        }

        fn mean(&self) -> f64 {
            self.total_ns as f64 / self.samples.len() as f64
        }
    }

    // Thread-safe metrics wrapper
    struct ThreadMetrics {
        metrics: Mutex<LatencyMetrics>,
    }

    impl ThreadMetrics {
        fn new() -> Self {
            Self {
                metrics: Mutex::new(LatencyMetrics::new()),
            }
        }

        fn update(&self, latency: u64) {
            if let Ok(mut metrics) = self.metrics.lock() {
                metrics.update(latency);
            }
        }

        fn get_metrics(&self) -> Option<LatencyMetrics> {
            self.metrics.lock().ok().map(|metrics| metrics.clone())
        }
    }

    // Make LatencyMetrics cloneable for thread safety
    impl Clone for LatencyMetrics {
        fn clone(&self) -> Self {
            Self {
                min_ns: self.min_ns,
                max_ns: self.max_ns,
                total_ns: self.total_ns,
                samples: self.samples.clone(),
            }
        }
    }

    // Producer function
    fn producer_thread(
        metrics: Arc<ThreadMetrics>,
        table: Arc<Table>,
        running: Arc<AtomicBool>,
    ) {
        let mut record = HashMap::with_capacity(1);
        while running.load(Ordering::Relaxed) {
            record.clear();
            record.insert("data", Vec::from(42u64.to_le_bytes()).into_boxed_slice());
            
            let start = Instant::now();
            if table.write_record(record.clone()) {
                let latency = start.elapsed().as_nanos() as u64;
                metrics.update(latency);
            }
            thread::yield_now();
        }
    }

    // Consumer function
    fn consumer_thread(
        metrics: Arc<ThreadMetrics>,
        table: Arc<Table>,
        running: Arc<AtomicBool>,
    ) {
        while running.load(Ordering::Relaxed) {
            let start = Instant::now();
            if let Some(_) = table.read_one_record() {
                let latency = start.elapsed().as_nanos() as u64;
                metrics.update(latency);
            }
            thread::yield_now();
        }
    }

    #[test]
    fn test_system_latencies() {
        println!("\nRunning Detailed Latency Analysis");
        println!("================================");

        // Pre-allocate all test data
        let test_data = (0..TEST_ITERATIONS)
            .map(|i| i as u64)
            .collect::<Vec<_>>();

        // Setup minimal table for latency testing
        let mut fields = HashMap::with_capacity(1);
        fields.insert("data", FieldConfig {
            field_size_bytes: 8,
            ring_capacity: RING_BUFFER_SIZE,
        });

        let table_config = TableConfig { fields };
        let table = Arc::new(Table::new("latency_test", table_config));
        
        // Pre-allocate buffers for all metrics
        let mut write_metrics = LatencyMetrics::new();
        let mut read_metrics = LatencyMetrics::new();

        // Warmup phase with zero allocations
        println!("Warming up...");
        {
            let mut record = HashMap::with_capacity(1);
            for _ in 0..WARMUP_ITERATIONS {
                record.clear();
                record.insert("data", Vec::from(42u64.to_le_bytes()).into_boxed_slice());
                black_box(table.write_record(record.clone()));
                black_box(table.read_one_record());
            }
        }

        // Single-threaded latency test with zero allocations
        println!("Running single-threaded latency test...");
        {
            let mut record = HashMap::with_capacity(1);
            for &data in test_data.iter() {
                // Write latency
                record.clear();
                record.insert("data", Vec::from(data.to_le_bytes()).into_boxed_slice());
                
                let start = Instant::now();
                table.write_record(record.clone());
                let latency = start.elapsed().as_nanos() as u64;
                write_metrics.update(latency);

                // Read latency
                let start = Instant::now();
                let result = table.read_one_record();
                let latency = start.elapsed().as_nanos() as u64;
                read_metrics.update(latency);
                black_box(result);
            }
        }

        // Multi-threaded latency test
        println!("Running multi-threaded latency test...");
        let running = Arc::new(AtomicBool::new(true));
        
        // Create thread metrics
        let producer_metrics = Arc::new(ThreadMetrics::new());
        let consumer_metrics = Arc::new(ThreadMetrics::new());

        // Spawn threads
        let producer = {
            let metrics = Arc::clone(&producer_metrics);
            let table = Arc::clone(&table);
            let running = Arc::clone(&running);
            thread::spawn(move || {
                producer_thread(metrics, table, running);
            })
        };

        let consumer = {
            let metrics = Arc::clone(&consumer_metrics);
            let table = Arc::clone(&table);
            let running = Arc::clone(&running);
            thread::spawn(move || {
                consumer_thread(metrics, table, running);
            })
        };

        // Run for a fixed duration
        thread::sleep(Duration::from_secs(5));
        running.store(false, Ordering::Release);

        // Wait for threads to complete
        producer.join().unwrap();
        consumer.join().unwrap();

        // Get results
        let producer_results = producer_metrics.get_metrics().unwrap();
        let consumer_results = consumer_metrics.get_metrics().unwrap();

        // Print results
        println!("\nSingle-threaded Write Latencies:");
        print_metrics(&write_metrics);

        println!("\nSingle-threaded Read Latencies:");
        print_metrics(&read_metrics);

        println!("\nMulti-threaded Write Latencies:");
        print_metrics(&producer_results);

        println!("\nMulti-threaded Read Latencies:");
        print_metrics(&consumer_results);
    }

    fn print_metrics(metrics: &LatencyMetrics) {
        println!("  Min latency: {} ns", metrics.min_ns);
        println!("  Max latency: {} ns", metrics.max_ns);
        println!("  Mean latency: {:.2} ns", metrics.mean());
        
        println!("  Percentiles:");
        for &p in PERCENTILES {
            println!("    P{:.2}: {} ns", p, metrics.percentile(p));
        }
    }

    #[test]
    fn test_per_instruction_latencies() {
        println!("\nPer-Instruction Latency Analysis");
        println!("===============================");

        // Setup minimal table for latency testing
        let mut fields = HashMap::with_capacity(1);
        fields.insert("data", FieldConfig {
            field_size_bytes: 8,
            ring_capacity: RING_BUFFER_SIZE,
        });

        let table_config = TableConfig { fields };
        let table = Arc::new(Table::new("instruction_latency_test", table_config));
        
        // Pre-allocate test data
        let mut record = HashMap::with_capacity(1);
        let test_data = Vec::from(42u64.to_le_bytes()).into_boxed_slice();
        
        // Warmup phase
        for _ in 0..1000 {
            record.clear();
            record.insert("data", test_data.clone());
            black_box(table.write_record(record.clone()));
            black_box(table.read_one_record());
        }

        // Measure individual instruction latencies
        let iterations = 10_000;
        let mut latencies = HashMap::new();

        // 1. HashMap creation latency
        {
            let mut total_ns = 0;
            for _ in 0..iterations {
                let start = Instant::now();
                let _map = HashMap::<&'static str, Box<[u8]>>::with_capacity(1);
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("HashMap Creation", total_ns / iterations);
        }

        // 2. Data cloning latency
        {
            let mut total_ns = 0;
            for _ in 0..iterations {
                let start = Instant::now();
                let _cloned = test_data.clone();
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("Data Clone", total_ns / iterations);
        }

        // 3. HashMap insertion latency
        {
            let mut map = HashMap::with_capacity(1);
            let mut total_ns = 0;
            for _ in 0..iterations {
                map.clear();
                let start = Instant::now();
                map.insert("data", test_data.clone());
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("HashMap Insert", total_ns / iterations);
        }

        // 4. Write record latency (no contention)
        {
            let mut total_ns = 0;
            for _ in 0..iterations {
                record.clear();
                record.insert("data", test_data.clone());
                let start = Instant::now();
                black_box(table.write_record(record.clone()));
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("Write Record (No Contention)", total_ns / iterations);
        }

        // 5. Read record latency (no contention)
        {
            let mut total_ns = 0;
            for _ in 0..iterations {
                let start = Instant::now();
                black_box(table.read_one_record());
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("Read Record (No Contention)", total_ns / iterations);
        }

        // 6. Write record latency (with contention)
        {
            let running = Arc::new(AtomicBool::new(true));
            let table_clone = Arc::clone(&table);
            let running_clone = Arc::clone(&running);
            
            // Create contention with a background thread
            let background = thread::spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    let mut record = HashMap::with_capacity(1);
                    record.insert("data", Vec::from(42u64.to_le_bytes()).into_boxed_slice());
                    black_box(table_clone.write_record(record));
                    thread::yield_now();
                }
            });

            let mut total_ns = 0;
            for _ in 0..iterations {
                record.clear();
                record.insert("data", test_data.clone());
                let start = Instant::now();
                black_box(table.write_record(record.clone()));
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("Write Record (With Contention)", total_ns / iterations);
            
            running.store(false, Ordering::Release);
            background.join().unwrap();
        }

        // 7. Read record latency (with contention)
        {
            let running = Arc::new(AtomicBool::new(true));
            let table_clone = Arc::clone(&table);
            let running_clone = Arc::clone(&running);
            
            // Create contention with a background thread
            let background = thread::spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    black_box(table_clone.read_one_record());
                    thread::yield_now();
                }
            });

            let mut total_ns = 0;
            for _ in 0..iterations {
                let start = Instant::now();
                black_box(table.read_one_record());
                total_ns += start.elapsed().as_nanos() as u64;
            }
            latencies.insert("Read Record (With Contention)", total_ns / iterations);
            
            running.store(false, Ordering::Release);
            background.join().unwrap();
        }

        // Print results
        println!("\nPer-Instruction Average Latencies:");
        println!("=================================");
        let mut sorted_latencies: Vec<_> = latencies.iter().collect();
        sorted_latencies.sort_by_key(|&(_, &v)| v);
        
        for (operation, latency) in sorted_latencies {
            println!("{:<30} {:>8} ns", operation, latency);
        }
    }
} 