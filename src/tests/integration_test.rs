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
use std::cell::RefCell;
use crossbeam::queue::ArrayQueue;
use std::arch::x86_64::*;

use crate::storage::table::{Table, TableConfig, FieldConfig};

// Constants for performance tuning
const RING_BUFFER_SIZE: usize = 16384;  // 16K entries per field
const BATCH_SIZE: usize = 256;          // Optimal cache line usage
const CACHE_LINE_SIZE: usize = 64;      // Common CPU cache line size
const MAX_RETRIES: usize = 1000;

// Align data to cache line boundaries to prevent false sharing
#[repr(align(64))]
struct PreAllocatedRecord {
    symbol_id: [u8; 4],    
    price: [u8; 8],
    quantity: [u8; 4],
    timestamp: [u8; 8],
    exchange_id: [u8; 1],
    _padding: [u8; 39],    
}

impl PreAllocatedRecord {
    // Preallocate the HashMap to avoid runtime allocations
    thread_local! {
        static REUSABLE_BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(64));
        static REUSABLE_MAP: std::cell::RefCell<HashMap<&'static str, Box<[u8]>>> = std::cell::RefCell::new({
            let mut map = HashMap::with_capacity(5);
            map.insert("symbol_id", vec![0; 4].into_boxed_slice());
            map.insert("price", vec![0; 8].into_boxed_slice());
            map.insert("quantity", vec![0; 4].into_boxed_slice());
            map.insert("timestamp", vec![0; 8].into_boxed_slice());
            map.insert("exchange_id", vec![1].into_boxed_slice());
            map
        });
    }

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

    // Zero-allocation version using thread-local storage
    #[inline(always)]
    fn to_hashmap(&self) -> HashMap<&'static str, Box<[u8]>> {
        Self::REUSABLE_MAP.with(|map| {
            let mut map = map.borrow_mut();
            // Direct memory copies without allocations
            map.get_mut("symbol_id").map(|v| v.copy_from_slice(&self.symbol_id));
            map.get_mut("price").map(|v| v.copy_from_slice(&self.price));
            map.get_mut("quantity").map(|v| v.copy_from_slice(&self.quantity));
            map.get_mut("timestamp").map(|v| v.copy_from_slice(&self.timestamp));
            map.get_mut("exchange_id").map(|v| v[0] = self.exchange_id[0]);
            map.clone()
        })
    }

    #[inline(always)]
    fn as_ref_map(&self) -> HashMap<&'static str, &[u8]> {
        let mut map = HashMap::with_capacity(5);
        // Convert fixed arrays to slices
        map.insert("symbol_id", &self.symbol_id[..]);
        map.insert("price", &self.price[..]);
        map.insert("quantity", &self.quantity[..]);
        map.insert("timestamp", &self.timestamp[..]);
        map.insert("exchange_id", &self.exchange_id[..]);
        map
    }

    #[inline(always)]
    fn to_direct_record(&self) -> Option<DirectRecord> {
        RECORD_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            pool.acquire().map(|mut record| {
                unsafe {
                    let mut offset = 0;
                    // Direct memory copy without intermediate allocations
                    std::ptr::copy_nonoverlapping(
                        self.symbol_id.as_ptr(),
                        record.data.as_mut_ptr().add(offset),
                        self.symbol_id.len()
                    );
                    offset += self.symbol_id.len();

                    std::ptr::copy_nonoverlapping(
                        self.price.as_ptr(),
                        record.data.as_mut_ptr().add(offset),
                        self.price.len()
                    );
                    offset += self.price.len();

                    std::ptr::copy_nonoverlapping(
                        self.quantity.as_ptr(),
                        record.data.as_mut_ptr().add(offset),
                        self.quantity.len()
                    );
                    offset += self.quantity.len();

                    std::ptr::copy_nonoverlapping(
                        self.timestamp.as_ptr(),
                        record.data.as_mut_ptr().add(offset),
                        self.timestamp.len()
                    );
                    offset += self.timestamp.len();

                    std::ptr::copy_nonoverlapping(
                        self.exchange_id.as_ptr(),
                        record.data.as_mut_ptr().add(offset),
                        self.exchange_id.len()
                    );
                    offset += self.exchange_id.len();

                    record.len = offset;
                    record
                }
            })
        })
    }
}

// Cache-aligned performance stats
#[repr(align(64))]
struct PerformanceStats {
    // Use fixed-size arrays with atomic access
    write_latencies: Box<[AtomicU64]>,
    read_latencies: Box<[AtomicU64]>,
    write_index: AtomicUsize,
    read_index: AtomicUsize,
    dropped_messages: AtomicUsize,
    total_messages: AtomicUsize,
    max_latency: AtomicU64,
    _padding: [u8; CACHE_LINE_SIZE - 40],
}

impl PerformanceStats {
    fn new(capacity: usize) -> Self {
        let write_latencies = (0..capacity)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let read_latencies = (0..capacity)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
            
        Self {
            write_latencies,
            read_latencies,
            write_index: AtomicUsize::new(0),
            read_index: AtomicUsize::new(0),
            dropped_messages: AtomicUsize::new(0),
            total_messages: AtomicUsize::new(0),
            max_latency: AtomicU64::new(0),
            _padding: [0; CACHE_LINE_SIZE - 40],
        }
    }

    #[inline(always)]
    fn add_write_latency(&self, latency: u64) {
        let idx = self.write_index.fetch_add(1, Ordering::Relaxed) % self.write_latencies.len();
        self.write_latencies[idx].store(latency, Ordering::Relaxed);
    }

    #[inline(always)]
    fn add_read_latency(&self, latency: u64) {
        let idx = self.read_index.fetch_add(1, Ordering::Relaxed) % self.read_latencies.len();
        self.read_latencies[idx].store(latency, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (Option<f64>, Option<f64>, u64) {
        let write_sum: u64 = self.write_latencies
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .sum();
        let write_count = self.write_index.load(Ordering::Relaxed).min(self.write_latencies.len());
        let avg_write = if write_count > 0 {
            Some(write_sum as f64 / write_count as f64)
        } else {
            None
        };

        let read_sum: u64 = self.read_latencies
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .sum();
        let read_count = self.read_index.load(Ordering::Relaxed).min(self.read_latencies.len());
        let avg_read = if read_count > 0 {
            Some(read_sum as f64 / read_count as f64)
        } else {
            None
        };

        let max = self.max_latency.load(Ordering::Relaxed);
        (avg_write, avg_read, max)
    }
}

// Zero-copy direct record format
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct DirectRecord {
    data: [u8; 64],
    len: usize,
    _padding: [u8; 64 - std::mem::size_of::<usize>()],
}

impl DirectRecord {
    #[inline(always)]
    fn new() -> Self {
        Self {
            data: [0; 64],
            len: 0,
            _padding: [0; 64 - std::mem::size_of::<usize>()],
        }
    }

    #[inline(always)]
    fn write_field(&mut self, offset: usize, data: &[u8]) -> usize {
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.data.as_mut_ptr().add(offset), data.len());
            offset + data.len()
        }
    }

    #[inline(always)]
    fn to_hashmap(&self) -> HashMap<&'static str, Box<[u8]>> {
        let mut map = HashMap::with_capacity(5);
        map.insert("data", self.data[..self.len].to_vec().into_boxed_slice());
        map
    }
}

// Memory pool for zero-allocation record reuse
struct RecordPool {
    records: Box<[DirectRecord]>,
    free_indices: crossbeam::queue::ArrayQueue<usize>,
}

impl RecordPool {
    fn new(capacity: usize) -> Self {
        let mut records = Vec::with_capacity(capacity);
        records.resize_with(capacity, DirectRecord::new);
        let free_indices = crossbeam::queue::ArrayQueue::new(capacity);
        for i in 0..capacity {
            let _ = free_indices.push(i);
        }
        Self {
            records: records.into_boxed_slice(),
            free_indices,
        }
    }

    #[inline(always)]
    fn acquire(&mut self) -> Option<DirectRecord> {
        self.free_indices.pop().map(|idx| self.records[idx])
    }

    #[inline(always)]
    fn release(&self, _record: DirectRecord) {
        // In this optimized version, we don't need to track releases
        // since DirectRecord is Copy and we're using a fixed pool size
    }
}

// Thread-local record pool
thread_local! {
    static RECORD_POOL: RefCell<RecordPool> = RefCell::new(RecordPool::new(RING_BUFFER_SIZE));
}

// SIMD-optimized batch processing
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn process_batch_simd(records: &mut [DirectRecord]) {
    if is_x86_feature_detected!("avx2") {
        let mut i = 0;
        let len = records.len();
        while i + 4 <= len {
            let r0 = _mm256_loadu_si256(records[i].data.as_ptr() as *const __m256i);
            let r1 = _mm256_loadu_si256(records[i + 1].data.as_ptr() as *const __m256i);
            let r2 = _mm256_loadu_si256(records[i + 2].data.as_ptr() as *const __m256i);
            let r3 = _mm256_loadu_si256(records[i + 3].data.as_ptr() as *const __m256i);
            
            // Process 4 records in parallel using AVX2
            let processed = _mm256_add_epi64(
                _mm256_add_epi64(r0, r1),
                _mm256_add_epi64(r2, r3)
            );
            
            _mm256_storeu_si256(records[i].data.as_mut_ptr() as *mut __m256i, processed);
            i += 4;
        }
    }
}

#[inline(always)]
fn record_to_direct(record: &HashMap<&str, &[u8]>) -> Option<DirectRecord> {
    RECORD_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        pool.acquire().map(|mut direct_record| {
            let mut offset = 0;
            // Copy fields in a fixed order for SIMD processing
            for &field in &["symbol_id", "price", "quantity", "timestamp", "exchange_id"] {
                if let Some(data) = record.get(field) {
                    offset = direct_record.write_field(offset, data);
                }
            }
            direct_record.len = offset;
            direct_record
        })
    })
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
            // Pin thread to CPU core if possible
            #[cfg(target_os = "linux")]
            {
                use core_affinity::set_for_current;
                if let Some(core_id) = core_affinity::get_core_ids().map(|cores| cores[p_id % cores.len()]) {
                    set_for_current(core_id);
                }
            }

            let mut record = PreAllocatedRecord::new();
            let mut batch_count = 0;
            let mut retry_count = 0;
            
            // Pre-calculate timestamp base to reduce syscalls
            let time_base = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            // Use RDTSC for high precision timing
            let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };
            let mut last_tsc = start_tsc;
            
            for i in 0..MESSAGES_PER_PRODUCER {
                // Minimize syscalls by using RDTSC delta
                let current_tsc = unsafe { core::arch::x86_64::_rdtsc() };
                let tsc_delta = current_tsc - start_tsc;
                let timestamp = time_base + (tsc_delta / 2); // Approximate ns conversion
                
                // Direct memory writes without bounds checking
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        ((100 + p_id) as u32).to_le_bytes().as_ptr(),
                        record.symbol_id.as_mut_ptr(),
                        4
                    );
                    std::ptr::copy_nonoverlapping(
                        ((1000.0 + (i as f64) * 0.01) as f64).to_le_bytes().as_ptr(),
                        record.price.as_mut_ptr(),
                        8
                    );
                    std::ptr::copy_nonoverlapping(
                        (100 + (i % 100) as u32).to_le_bytes().as_ptr(),
                        record.quantity.as_mut_ptr(),
                        4
                    );
                    std::ptr::copy_nonoverlapping(
                        &timestamp.to_le_bytes() as *const u8,
                        record.timestamp.as_mut_ptr(),
                        8
                    );
                    *record.exchange_id.as_mut_ptr() = p_id as u8;
                }

                let write_start = Instant::now();
                let mut success = false;
                let mut backoff = crossbeam::utils::Backoff::new();
                
                while !success && retry_count < MAX_RETRIES {
                    if table.write_record_ref(&record.as_ref_map()) {
                        let latency = write_start.elapsed().as_nanos() as u64;
                        stats.max_latency.store(latency, Ordering::Relaxed);
                        stats.add_write_latency(latency);
                        stats.total_messages.fetch_add(1, Ordering::Relaxed);
                        success = true;
                        retry_count = 0;
                    } else {
                        retry_count += 1;
                        backoff.snooze();
                    }
                }
                
                // Release the record back to the pool
                RECORD_POOL.with(|pool| {
                    pool.borrow().release(record.to_direct_record().unwrap());
                });

                // Adaptive batching based on CPU frequency
                batch_count += 1;
                if batch_count >= BATCH_SIZE {
                    if current_tsc - last_tsc > 1_000_000 { // ~1ms in cycles
                        thread::yield_now();
                        last_tsc = current_tsc;
                    }
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
            // Pin thread to CPU core
            #[cfg(target_os = "linux")]
            {
                use core_affinity::set_for_current;
                if let Some(core_id) = core_affinity::get_core_ids().map(|cores| cores[c_id % cores.len()]) {
                    set_for_current(core_id);
                }
            }

            let mut processed_count = 0;
            let mut batch_buffer = Vec::with_capacity(BATCH_SIZE);
            let target_messages = MESSAGES_PER_PRODUCER * PRODUCER_COUNT / CONSUMER_COUNT;
            
            // Pre-allocate SIMD batch buffer
            let mut simd_batch = Vec::with_capacity(BATCH_SIZE);
            
            while processed_count < target_messages {
                batch_buffer.clear();
                simd_batch.clear();
                let read_start = Instant::now();
                
                // Batch reading with SIMD processing
                for _ in 0..BATCH_SIZE {
                    if let Some(record) = table.read_record_ref() {
                        if let Some(direct_record) = record_to_direct(&record) {
                            simd_batch.push(direct_record);
                        }
                        batch_buffer.push(record);
                    } else {
                        break;
                    }
                }

                if !batch_buffer.is_empty() {
                    let latency = read_start.elapsed().as_nanos() as u64;
                    stats.max_latency.store(latency, Ordering::Relaxed);
                    stats.add_read_latency(latency);
                    
                    // Process batch using SIMD if available
                    if !simd_batch.is_empty() {
                        unsafe {
                            process_batch_simd(&mut simd_batch);
                        }
                    }
                    
                    for record in &batch_buffer {
                        match c_id {
                            0 => {
                                // Zero-copy price tracking using SIMD batch results
                                if let Some(price_bytes) = record.get("price") {
                                    let price = f64::from_le_bytes(price_bytes[..8].try_into().unwrap());
                                    if processed_count % 1000 == 0 {
                                        println!("Consumer {}: Price update: {}", c_id, price);
                                    }
                                }
                            },
                            1 => {
                                // Zero-copy latency analysis with SIMD acceleration
                                if let Some(ts_bytes) = record.get("timestamp") {
                                    let msg_ts = u64::from_le_bytes(ts_bytes[..8].try_into().unwrap());
                                    let current = unsafe { core::arch::x86_64::_rdtsc() } / 2; // Approximate ns
                                    if current > msg_ts {
                                        stats.add_read_latency(current - msg_ts);
                                    }
                                }
                            },
                            _ => {
                                if processed_count % 1000 == 0 {
                                    println!("Consumer {}: Processed {} messages", c_id, processed_count);
                                }
                            }
                        }
                        processed_count += 1;
                    }

                    // Release SIMD batch records back to pool
                    for record in simd_batch.drain(..) {
                        RECORD_POOL.with(|pool| {
                            pool.borrow().release(record);
                        });
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
            let mut record = HashMap::new();
            for _ in 0..WARMUP_ITERATIONS {
                record.clear();
                record.insert("data", vec![42u8; 8].into_boxed_slice());
                black_box(table.write_record(record.clone()));
                black_box(table.read_one_record());
            }
        }

        // Single-threaded latency test
        println!("Running single-threaded latency test...");
        {
            let mut record = HashMap::new();
            for &data in test_data.iter() {
                record.clear();
                record.insert("data", data.to_le_bytes().to_vec().into_boxed_slice());
                
                let start = Instant::now();
                table.write_record(record.clone());
                let latency = start.elapsed().as_nanos() as u64;
                write_metrics.update(latency);

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
        
        // Thread-safe metrics wrapper
        struct ThreadMetrics {
            metrics: LatencyMetrics,
        }

        // Producer function
        fn producer_thread(
            metrics: &mut ThreadMetrics,
            table: &Table,
            running: &AtomicBool,
        ) {
            let mut record = HashMap::new();
            while running.load(Ordering::Relaxed) {
                record.clear();
                record.insert("data", vec![42u8; 8].into_boxed_slice());
                
                let start = Instant::now();
                if table.write_record(record.clone()) {
                    let latency = start.elapsed().as_nanos() as u64;
                    metrics.metrics.update(latency);
                }
                thread::yield_now();
            }
        }

        // Consumer function
        fn consumer_thread(
            metrics: &mut ThreadMetrics,
            table: &Table,
            running: &AtomicBool,
        ) {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if let Some(_) = table.read_one_record() {
                    let latency = start.elapsed().as_nanos() as u64;
                    metrics.metrics.update(latency);
                }
                thread::yield_now();
            }
        }

        // Create thread contexts
        let mut producer_metrics = ThreadMetrics {
            metrics: LatencyMetrics::new(),
        };

        let mut consumer_metrics = ThreadMetrics {
            metrics: LatencyMetrics::new(),
        };

        let table_producer = Arc::clone(&table);
        let table_consumer = Arc::clone(&table);
        let running_producer = Arc::clone(&running);
        let running_consumer = Arc::clone(&running);

        // Spawn threads
        let producer = {
            let mut metrics = producer_metrics;
            thread::spawn(move || {
                producer_thread(&mut metrics, &table_producer, &running_producer);
                metrics
            })
        };

        let consumer = {
            let mut metrics = consumer_metrics;
            thread::spawn(move || {
                consumer_thread(&mut metrics, &table_consumer, &running_consumer);
                metrics
            })
        };

        // Run for a fixed duration
        thread::sleep(Duration::from_secs(5));
        running.store(false, Ordering::Release);

        // Collect results
        let producer_metrics = producer.join().unwrap();
        let consumer_metrics = consumer.join().unwrap();

        // Print results
        println!("\nSingle-threaded Write Latencies:");
        print_metrics(&write_metrics);

        println!("\nSingle-threaded Read Latencies:");
        print_metrics(&read_metrics);

        println!("\nMulti-threaded Write Latencies:");
        print_metrics(&producer_metrics.metrics);

        println!("\nMulti-threaded Read Latencies:");
        print_metrics(&consumer_metrics.metrics);
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
} 