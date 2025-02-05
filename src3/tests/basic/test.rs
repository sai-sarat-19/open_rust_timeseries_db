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
    let mut total_writes = 0;
    for handle in producer_handles {
        let latencies = handle.join().unwrap();
        total_writes += latencies.len();
        write_latencies.extend(latencies);
    }
    
    let mut total_reads = 0;
    for handle in consumer_handles {
        let latencies = handle.join().unwrap();
        total_reads += latencies.len();
        read_latencies.extend(latencies);
    }

    // Analyze latencies
    write_latencies.sort_unstable();
    read_latencies.sort_unstable();

    let write_stats = calculate_latency_stats(&write_latencies);
    let read_stats = calculate_latency_stats(&read_latencies);

    println!("\nSystem Performance Statistics");
    println!("---------------------------");
    println!("Total Operations:");
    println!("  Writes: {}", total_writes);
    println!("  Reads: {}", total_reads);
    println!("\nWrite Latencies (cycles):");
    print_latency_stats(&write_stats);
    println!("\nRead Latencies (cycles):");
    print_latency_stats(&read_stats);

    // Verify the system worked correctly
    assert!(total_writes > 0, "No writes were performed");
    assert!(total_reads > 0, "No reads were performed");
    assert!(total_reads <= total_writes, "Read more records than were written");
    
    // Verify buffer behavior is correct
    for &token in &instruments {
        unsafe {
            // L2Trade and Reference buffers should be empty as we never wrote to them
            assert!(manager.read(token, BufferType::L2Trade).is_none(), 
                "L2Trade buffer should be empty for instrument {}", token);
            assert!(manager.read(token, BufferType::Reference).is_none(),
                "Reference buffer should be empty for instrument {}", token);
        }
    }
} 