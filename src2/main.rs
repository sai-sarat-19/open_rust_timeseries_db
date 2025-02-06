use std::thread;
use std::time::Duration;
use std::sync::Arc;

use ultra_low_latency_db::{
    core::market_data::MarketDataRecord,
    db::ultra_low_latency_db::UltraLowLatencyDB,
};

fn main() {
    println!("Starting Ultra-Low-Latency DB Example");
    
    // Create a new DB instance with 16K capacity
    // We use Arc here because we need to share ownership across threads
    let db = Arc::new(UltraLowLatencyDB::<MarketDataRecord>::new(16384));

    // Spawn producer thread with a reference
    let producer_db = Arc::clone(&db);
    let producer = thread::spawn(move || {
        for i in 0..100_000 {
            let record = MarketDataRecord::new(
                101,                                // symbol_id
                10_000.0 + i as f64,               // bid_price
                10_000.1 + i as f64,               // ask_price
                100,                               // bid_size
                100,                               // ask_size
                10_000.05 + i as f64,              // last_price
                100,                               // last_size
                i as u64,                          // timestamp
                i as u64,                          // sequence_num
                0,                                 // flags
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
                    println!(
                        "Consumed {} records, bid: {}, ask: {}", 
                        count, 
                        record.bid_price,
                        record.ask_price
                    );
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
 