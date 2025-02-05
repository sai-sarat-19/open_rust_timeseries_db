#[cfg(test)]
mod tests {
    use crate::{
        UltraLowLatencyRecord,
        UltraLowLatencyDB,
        memory::zero_alloc_ring_buffer::rdtsc_serialized,
    };
    use std::hint::black_box;
    use std::sync::Arc;

    #[test]
    fn test_basic_operations() {
        let db = UltraLowLatencyDB::new(16384);
        
        // Test write and read
        let record = UltraLowLatencyRecord::new(1, 100.0, 1000, 0, 0);
        assert!(db.write(&record));
        
        let read_record = db.read().expect("Should read record");
        assert_eq!(read_record.symbol_id, 1);
        assert_eq!(read_record.price, 100.0);
        assert_eq!(read_record.quantity, 1000);
    }

    #[test]
    fn test_buffer_full() {
        // Create a buffer with size 3 to test full condition
        let db = UltraLowLatencyDB::new(3);
        let record = UltraLowLatencyRecord::new(1, 100.0, 1000, 0, 0);
        
        // Should be able to write capacity-1 records
        assert!(db.write(&record), "First write should succeed");
        assert!(db.write(&record), "Second write should succeed");
        // The third write should fail as we need one empty slot
        assert!(!db.write(&record), "Third write should fail as buffer is full");
        
        // After reading one record, we should be able to write again
        assert!(db.read().is_some(), "Should be able to read a record");
        assert!(db.write(&record), "Should be able to write after reading");
    }

    #[test]
    fn test_latency() {
        println!("\nRunning Ultra-Low Latency Test");
        println!("==============================");

        let db = UltraLowLatencyDB::new(16384);
        let mut record = UltraLowLatencyRecord::new(1, 100.0, 1000, 0, 0);

        // Warmup
        for _ in 0..1_000_000 {
            black_box(db.write(&record));
            black_box(db.read());
        }

        let mut write_latencies = Vec::with_capacity(1_000_000);
        let mut read_latencies = Vec::with_capacity(1_000_000);

        // Measure write latencies
        for i in 0..1_000_000 {
            record.timestamp = i as u64;
            let start = unsafe { rdtsc_serialized() };
            black_box(db.write(&record));
            let end = unsafe { rdtsc_serialized() };
            write_latencies.push(end - start);
        }

        // Measure read latencies
        for _ in 0..1_000_000 {
            let start = unsafe { rdtsc_serialized() };
            black_box(db.read());
            let end = unsafe { rdtsc_serialized() };
            read_latencies.push(end - start);
        }

        write_latencies.sort_unstable();
        read_latencies.sort_unstable();

        let w_min = write_latencies[0];
        let w_max = *write_latencies.last().unwrap();
        let w_median = write_latencies[write_latencies.len() / 2];
        let w_p99 = write_latencies[(write_latencies.len() as f64 * 0.99) as usize];

        println!("\nWrite Latencies (cycles):");
        println!("  Min: {}", w_min);
        println!("  Median: {}", w_median);
        println!("  P99: {}", w_p99);
        println!("  Max: {}", w_max);

        let r_min = read_latencies[0];
        let r_max = *read_latencies.last().unwrap();
        let r_median = read_latencies[read_latencies.len() / 2];
        let r_p99 = read_latencies[(read_latencies.len() as f64 * 0.99) as usize];

        println!("\nRead Latencies (cycles):");
        println!("  Min: {}", r_min);
        println!("  Median: {}", r_median);
        println!("  P99: {}", r_p99);
        println!("  Max: {}", r_max);
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;
        
        // For multi-threaded tests, we need Arc
        let db = Arc::new(UltraLowLatencyDB::new(1024));
        let mut handles = vec![];

        // Spawn multiple writer threads
        for i in 0..4 {
            let db = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let record = UltraLowLatencyRecord::new(i as u32, 100.0, 1000, 0, 0);
                db.write(&record)
            });
            handles.push(handle);
        }

        // Spawn multiple reader threads
        for _ in 0..4 {
            let db = Arc::clone(&db);
            let handle = thread::spawn(move || {
                db.read().is_some()
            });
            handles.push(handle);
        }

        // All threads should complete without panicking
        for handle in handles {
            handle.join().unwrap();
        }
    }
} 