use std::thread;
use std::time::Duration;
use std::sync::Arc;

use ultra_low_latency_db::{
    UltraLowLatencyDB,
    UltraLowLatencyRecord,
};

fn main() {
    println!("Starting Ultra-Low-Latency DB Example");
    
    // Create a new DB instance with 16K capacity
    // We use Arc here because we need to share ownership across threads
    let db = Arc::new(UltraLowLatencyDB::new(16384));

    // Spawn producer thread with a reference
    let producer_db = Arc::clone(&db);
    let producer = thread::spawn(move || {
        for i in 0..100_000 {
            let record = UltraLowLatencyRecord::new(
                101,  // symbol_id
                10_000.0 + i as f64,  // price
                100,  // quantity
                i as u64,  // timestamp
                0,  // flags
            );
            
            while !producer_db.write(&record) {
                thread::yield_now();  // Buffer full, yield to other threads
            }
            
            if i % 10_000 == 0 {
                println!("Produced {} records", i);
            }
        }
        println!("Producer finished");
    });

    // Spawn consumer thread with a reference
    let consumer_db = Arc::clone(&db);
    let consumer = thread::spawn(move || {
        let mut count = 0;
        while count < 100_000 {
            if let Some(record) = consumer_db.read() {
                count += 1;
                if count % 10_000 == 0 {
                    println!("Consumed {} records, last price: {}", count, record.price);
                }
            } else {
                thread::sleep(Duration::from_micros(10));  // Buffer empty, wait briefly
            }
        }
        println!("Consumer finished");
    });

    // Wait for both threads to complete
    producer.join().unwrap();
    consumer.join().unwrap();
    
    println!("Example completed successfully!");
} 