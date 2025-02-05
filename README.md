# open_rust_timeseries_db


## Requirements


1. Requirements
Ultra-Low Latency:

Microsecond to low-millisecond overhead.
Minimal atomic operations or locks, favoring lock-free or near-lock-free concurrency.
Zero-copy or near-zero-copy data paths (store Vec<u8> directly).
Ephemeral, In-Memory Storage:

Data stored in ring buffers in RAM (no built-in persistence).
Overflow Policy: By default, if buffers are full, either the producer’s insert fails or you implement “drop oldest” logic.
Multi-Producer, Multi-Consumer (MPMC):

Multiple threads can write (produce) concurrently into each field’s ring buffer.
Multiple threads can read (consume) concurrently.
Field-by-Field Ring Buffers:

Each table has multiple fields; each field has its own ring buffer.
Different fields can have different capacities (customizable).
Scalable for Real-Time / HFT**:

Possibly thousands of writes per second to tens/hundreds of thousands.
Ability to handle concurrent “producers” from multiple feed sources.
Consumer(s) can do analytics, real-time lookups, or forward data further.
2. UML Class Diagram
Below is a PlantUML snippet representing:

The LowLatencyMpmcRing<T> ring buffer (the heart of the concurrency design).
The Table that holds multiple ring buffers (one per field).
TableConfig & FieldConfig controlling buffer capacities.
Copy-paste into PlantUML or a .puml file to visualize:

text
Copy
Edit
@startuml
skinparam monochrome true
skinparam shadowing false
skinparam defaultFontName Arial
skinparam defaultFontSize 12

title Low-Latency MPMC In-Memory DB (Ephemeral)

package "Memory" {
  class LowLatencyMpmcRing<T> {
    + try_enqueue(item: T): bool
    + try_dequeue(): Option<T>
    + is_empty(): bool
    + is_full(): bool
    - buffer: Box<[Slot<T>]>
    - capacity: usize
    - producer_index: AtomicUsize
    - consumer_index: AtomicUsize
    - mask: usize
  }

  class Slot<T> {
    + sequence: AtomicUsize
    + value: UnsafeCell<Option<T>>
  }
}

package "Storage" {
  class TableConfig {
    + fields: HashMap<String, FieldConfig>
  }

  class FieldConfig {
    + field_size_bytes: usize
    + ring_capacity: usize
  }

  class Table {
    + write_record(data: &HashMap<String, Vec<u8>>): bool
    + read_one_record(): Option<HashMap<String, Vec<u8>>>
    + record_count: AtomicUsize
    - field_buffers: DashMap<String, Arc<LowLatencyMpmcRing<Vec<u8>>>>
    - field_configs: HashMap<String, FieldConfig>
  }
}

note right of LowLatencyMpmcRing<T>
  Lock-free, sequence-based MPMC
  Minimal CAS ops
  Good for high contention
  ~Disruptor / Vyukov approach
end note

note right of Table
  One MPMC ring per field
  Custom ring capacity
  Multiple producers & consumers
  Ephemeral, no persistence
end note

LowLatencyMpmcRing<T> --> Slot<T>: has many
TableConfig --> FieldConfig: has many
Table --> TableConfig
Table --> "uses" LowLatencyMpmcRing
@enduml
Diagram Overview
LowLatencyMpmcRing<T>: The core data structure for concurrency.
Slot<T>: Internal ring slot with a sequence counter.
Table: Manages a DashMap of field name -> ring buffer. For each field, a LowLatencyMpmcRing<Vec<u8>> is created, sized by that field’s ring capacity.
TableConfig / FieldConfig: Custom capacities and field sizes.
3. Key Functions & Implementation
Below are the final code snippets for:

LowLatencyMpmcRing<T>: A sequence-based ring buffer.
Table: Using multiple ring buffers for ephemeral data storage.
An example main.rs usage for multiple producers/consumers.
You can place these snippets in your src/memory/ and src/storage/ folders, then integrate them into your project.

3.1 LowLatencyMpmcRing<T> (Sequence-Based)
File: src/memory/low_latency_mpmc_ring.rs

rust
Copy
Edit
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<Option<T>>,
}

/// A low-latency, sequence-based MPMC ring buffer.
/// Inspired by the Disruptor and Vyukov's bounded MPMC queue.
pub struct LowLatencyMpmcRing<T> {
    buffer: Box<[Slot<T>]>,
    capacity: usize,
    mask: usize,

    producer_index: AtomicUsize,
    consumer_index: AtomicUsize,
}

impl<T> LowLatencyMpmcRing<T> {
    /// Create a new ring of given capacity (ideally a power of two).
    pub fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        for i in 0..capacity {
            vec.push(Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(None),
            });
        }
        Self {
            buffer: vec.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            producer_index: AtomicUsize::new(0),
            consumer_index: AtomicUsize::new(0),
        }
    }

    /// Try to enqueue (produce) one item. Returns false if buffer is full.
    pub fn try_enqueue(&self, item: T) -> bool {
        loop {
            let seq = self.producer_index.load(Ordering::Acquire);
            let slot_index = seq & self.mask;

            let slot = unsafe { self.buffer.get_unchecked(slot_index) };
            let slot_seq = slot.sequence.load(Ordering::Acquire);

            // Expect slot_seq == seq => slot is free
            if slot_seq == seq {
                // Attempt to claim next producer slot
                if self.producer_index.compare_exchange_weak(
                    seq, seq.wrapping_add(1),
                    Ordering::AcqRel, Ordering::Relaxed
                ).is_ok()
                {
                    // We got the slot, place the item
                    unsafe {
                        *slot.value.get() = Some(item);
                    }
                    // Mark slot ready for consumer
                    slot.sequence.store(seq.wrapping_add(1), Ordering::Release);
                    return true;
                }
            } else if slot_seq < seq {
                // If slot_seq < seq, the slot isn't free yet or ring is full
                return false;
            } else {
                // slot_seq > seq => we are behind, might spin
                std::hint::spin_loop();
            }
        }
    }

    /// Try to dequeue (consume) one item. Returns None if empty.
    pub fn try_dequeue(&self) -> Option<T> {
        loop {
            let seq = self.consumer_index.load(Ordering::Acquire);
            let slot_index = seq & self.mask;

            let slot = unsafe { self.buffer.get_unchecked(slot_index) };
            let slot_seq = slot.sequence.load(Ordering::Acquire);

            // Expect slot_seq == seq+1 => it's ready for consumer
            if slot_seq == seq.wrapping_add(1) {
                // Attempt to advance consumer index
                if self.consumer_index.compare_exchange_weak(
                    seq, seq.wrapping_add(1),
                    Ordering::AcqRel, Ordering::Relaxed
                ).is_ok()
                {
                    // Read item
                    let val = unsafe { (*slot.value.get()).take() };
                    // Mark slot free for next cycle
                    slot.sequence.store(seq.wrapping_add(self.capacity), Ordering::Release);
                    return val;
                }
            } else if slot_seq < seq.wrapping_add(1) {
                // It's not yet produced
                return None;
            } else {
                // slot_seq > seq+1 => behind, spin
                std::hint::spin_loop();
            }
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.consumer_index.load(Ordering::Acquire) == self.producer_index.load(Ordering::Acquire)
    }

    /// Check if full
    pub fn is_full(&self) -> bool {
        (self.producer_index.load(Ordering::Acquire)
         - self.consumer_index.load(Ordering::Acquire)) >= self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
3.2 Table with One Ring per Field
File: src/storage/table.rs

rust
Copy
Edit
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use crate::memory::low_latency_mpmc_ring::LowLatencyMpmcRing;

/// Per-field configuration: how many bytes each entry might have, plus ring capacity
#[derive(Clone)]
pub struct FieldConfig {
    pub field_size_bytes: usize,
    pub ring_capacity: usize,
}

/// A table config with multiple fields, each having a FieldConfig
#[derive(Clone)]
pub struct TableConfig {
    pub fields: HashMap<String, FieldConfig>,
}

/// The Table manages a set of MPMC ring buffers (one per field).
/// Each buffer stores `Vec<u8>` for that field's data.
pub struct Table {
    pub name: String,
    pub field_configs: HashMap<String, FieldConfig>,
    pub field_buffers: DashMap<String, Arc<LowLatencyMpmcRing<Vec<u8>>>>,
    pub record_count: AtomicUsize,
}

impl Table {
    pub fn new(name: String, config: TableConfig) -> Self {
        let table = Self {
            name,
            field_configs: config.fields.clone(),
            field_buffers: DashMap::new(),
            record_count: AtomicUsize::new(0),
        };

        // Create a ring for each field
        for (field_name, fc) in &config.fields {
            let ring = Arc::new(LowLatencyMpmcRing::new(fc.ring_capacity));
            table.field_buffers.insert(field_name.clone(), ring);
        }

        table
    }

    /// Write one record. We try to enqueue each field's data in the corresponding ring.
    /// Returns false if any field is full (cannot enqueue).
    pub fn write_record(&self, record: &HashMap<String, Vec<u8>>) -> bool {
        for (field_name, data) in record {
            if let Some(ring_arc) = self.field_buffers.get(field_name) {
                if !ring_arc.try_enqueue(data.clone()) {
                    // If any field is full, the entire record fails
                    return false;
                }
            }
        }
        self.record_count.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// Read one record by dequeuing from each field. Returns None if any field is empty.
    ///
    /// NOTE: With multiple producers & consumers, there's a risk that the "nth" item
    /// in each buffer doesn't line up perfectly for a single record. This naive approach
    /// tries to read from each field in lockstep. Real alignment might need extra logic.
    pub fn read_one_record(&self) -> Option<HashMap<String, Vec<u8>>> {
        let mut result = HashMap::new();
        for (field_name, ring_arc) in self.field_buffers.iter() {
            if let Some(bytes) = ring_arc.try_dequeue() {
                result.insert(field_name.clone(), bytes);
            } else {
                // If any field is empty, we can't build a "full" record
                return None;
            }
        }
        self.record_count.fetch_sub(1, Ordering::SeqCst);
        Some(result)
    }
}
3.3 Example Usage with Multiple Producers & Consumers
File: src/main.rs

rust
Copy
Edit
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use my_ultra_low_latency_db::{
    storage::table::{Table, TableConfig, FieldConfig},
};

fn main() {
    // 1) Define each field with ring capacities (prefer power-of-two: e.g., 8192)
    let mut fields = HashMap::new();
    fields.insert("symbol_id".to_string(), FieldConfig { field_size_bytes: 4, ring_capacity: 8192 });
    fields.insert("price".to_string(), FieldConfig { field_size_bytes: 8, ring_capacity: 8192 });
    fields.insert("quantity".to_string(), FieldConfig { field_size_bytes: 4, ring_capacity: 8192 });
    fields.insert("timestamp".to_string(), FieldConfig { field_size_bytes: 8, ring_capacity: 8192 });

    let table_config = TableConfig { fields };
    let table = Arc::new(Table::new("market_data".to_string(), table_config));

    // 2) Spawn multiple producer threads
    let producer_count = 2;
    let mut producers = Vec::new();
    for p_id in 0..producer_count {
        let table_clone = table.clone();
        let handle = thread::spawn(move || {
            for i in 0..50_000 {
                let mut record = HashMap::new();
                record.insert("symbol_id".to_string(), (100 + p_id).to_le_bytes().to_vec());
                record.insert("price".to_string(), (10_000 + i).to_le_bytes().to_vec());
                record.insert("quantity".to_string(), (i as u32).to_le_bytes().to_vec());
                record.insert("timestamp".to_string(), current_time_nanos().to_le_bytes().to_vec());

                // Try writing. If full, we skip or handle overflow.
                if !table_clone.write_record(&record) {
                    println!("Producer {}: Buffer is full, skipping record {}", p_id, i);
                }

                // Simulate feed rate
                thread::sleep(Duration::from_micros(50));
            }
            println!("Producer {} done.", p_id);
        });
        producers.push(handle);
    }

    // 3) Spawn multiple consumer threads
    let consumer_count = 2;
    let mut consumers = Vec::new();
    for c_id in 0..consumer_count {
        let table_clone = table.clone();
        let handle = thread::spawn(move || {
            loop {
                // Attempt to read one record
                if let Some(record) = table_clone.read_one_record() {
                    // For example, parse "symbol_id" or "price"
                    let sym_bytes = record.get("symbol_id").unwrap();
                    let symbol_id = u64::from_le_bytes(sym_bytes[..8].try_into().unwrap());
                    // ...
                } else {
                    // If empty, sleep a bit
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        consumers.push(handle);
    }

    // 4) Wait for producers to finish
    for p in producers {
        p.join().unwrap();
    }

    // Let consumers run a bit more
    thread::sleep(Duration::from_secs(2));

    // Check how many records are in flight
    let count = table.record_count.load(std::sync::atomic::Ordering::SeqCst);
    println!("Records in table: {}", count);

    println!("Done.");
}

// Utility to get current nanoseconds
fn current_time_nanos() -> u64 {
    use std::time::SystemTime;
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
        .unwrap().as_nanos() as u64
}
Notes
Multiple Producers each write_record(...) concurrently; each field uses try_enqueue.
Multiple Consumers each read_one_record(...) concurrently; each field uses try_dequeue.
This works in a lock-free manner, using minimal atomic CAS operations in the ring buffers.
Misalignment Risk: If you want a consistent “record” across multiple fields, you must ensure producers write them in the same “index.” This is feasible if each producer’s iteration is done in lockstep. Otherwise, the naive approach might have partial mismatches.
4. Concluding Remarks
This final design is low-latency due to a sequence-based ring buffer approach (fewer CAS loops than naive MPMC).
Each field has a custom ring capacity, supporting different retention depths.
Ephemeral: We store data in memory only, discarding new writes if full (or you can code a “drop-oldest” approach by forcibly incrementing consumer pointer).
Multi-Producer, Multi-Consumer: Ideal for concurrent ingestion from multiple sources and concurrent reading by analytics threads.
If you need strict record alignment across fields or advanced transaction consistency, you’d implement additional logic (e.g., storing entire records in a single ring or adding a sequence ID in each field). For typical HFT or real-time ephemeral usage, this design is often fast, flexible, and suitable for microsecond-level latencies.

Enjoy building your ultra-low-latency MPMC in-memory DB!











Search


ChatGPT can make mistakes. Check important info.