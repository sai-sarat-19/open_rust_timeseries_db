use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::sync::atomic::Ordering;

use open_rust_timeseries_db::storage::table::{Table, TableConfig, FieldConfig};

fn main() {
    // Create field configurations with static strings
    let mut fields = HashMap::new();
    fields.insert("symbol_id", FieldConfig { 
        field_size_bytes: 4, 
        ring_capacity: 8192 
    });
    fields.insert("price", FieldConfig { 
        field_size_bytes: 8, 
        ring_capacity: 8192 
    });
    fields.insert("quantity", FieldConfig { 
        field_size_bytes: 4, 
        ring_capacity: 8192 
    });
    fields.insert("timestamp", FieldConfig { 
        field_size_bytes: 8, 
        ring_capacity: 8192 
    });
    fields.insert("exchange_id", FieldConfig { 
        field_size_bytes: 1, 
        ring_capacity: 8192 
    });

    let table_config = TableConfig { fields };
    let table = Arc::new(Table::new("market_data", table_config));

    // Create producer threads
    let producer_count = 2;
    let mut producers = Vec::new();
    
    for p_id in 0..producer_count {
        let table_clone = table.clone();
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                // Create record with static strings and owned data
                let mut record = HashMap::new();
                let symbol_id = (100 + p_id as u32).to_le_bytes().to_vec().into_boxed_slice();
                let price = (10_000.0 + i as f64).to_le_bytes().to_vec().into_boxed_slice();
                let quantity = (i as u32).to_le_bytes().to_vec().into_boxed_slice();
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    .to_le_bytes()
                    .to_vec()
                    .into_boxed_slice();
                let exchange_id = vec![p_id as u8].into_boxed_slice();

                record.insert("symbol_id", symbol_id);
                record.insert("price", price);
                record.insert("quantity", quantity);
                record.insert("timestamp", timestamp);
                record.insert("exchange_id", exchange_id);

                if !table_clone.write_record(record) {
                    println!("Producer {}: Buffer full at iteration {}", p_id, i);
                }

                thread::sleep(Duration::from_micros(50));
            }
            println!("Producer {} done.", p_id);
        });
        producers.push(handle);
    }

    // Create consumer threads
    let consumer_count = 2;
    let mut consumers = Vec::new();
    
    for c_id in 0..consumer_count {
        let table_clone = table.clone();
        let handle = thread::spawn(move || {
            let mut count = 0;
            while count < 500 {
                if let Some(record) = table_clone.read_one_record() {
                    let sym_bytes = record.get("symbol_id").unwrap();
                    let symbol_id = u32::from_le_bytes(sym_bytes[..4].try_into().unwrap());
                    println!("Consumer {} read symbol_id: {}", c_id, symbol_id);
                    count += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            println!("Consumer {} done.", c_id);
        });
        consumers.push(handle);
    }

    // Wait for producers and consumers
    for p in producers {
        p.join().unwrap();
    }
    for c in consumers {
        c.join().unwrap();
    }

    println!("Records in table: {}", table.record_count.load(Ordering::SeqCst));
}

fn current_time_nanos() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
} 