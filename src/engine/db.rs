use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;

use crate::core::record::Record;
use crate::core::types::*;
use crate::memory::ring_buffer::RingBuffer;

/// Configuration for the database
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Capacity of each ring buffer
    pub buffer_capacity: usize,
    /// Number of symbol partitions
    pub num_partitions: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 16384, // 16K records per buffer
            num_partitions: 64,     // 64 partitions
        }
    }
}

/// Ultra-low-latency in-memory database optimized for HFT
pub struct Database {
    /// Ring buffers partitioned by symbol
    buffers: Arc<DashMap<SymbolId, Arc<RingBuffer>>>,
    /// Configuration
    config: DatabaseConfig,
    /// Next record ID
    next_id: AtomicU64,
}

impl Database {
    /// Creates a new database with the given configuration
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            buffers: Arc::new(DashMap::with_capacity(config.num_partitions)),
            config,
            next_id: AtomicU64::new(0),
        }
    }

    /// Creates a new database with default configuration
    pub fn default() -> Self {
        Self::new(DatabaseConfig::default())
    }

    /// Writes a record to the database
    /// Returns true if successful, false if buffer is full
    pub fn write(&self, symbol_id: SymbolId, price: f64, quantity: Quantity) -> bool {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let record = Record::with_current_time(id, symbol_id, price, quantity, 0);

        // Get or create buffer for symbol
        let buffer = self.buffers.entry(symbol_id).or_insert_with(|| {
            Arc::new(RingBuffer::new(self.config.buffer_capacity))
        }).clone();

        // Write record
        unsafe { buffer.write(&record) }
    }

    /// Reads the latest record for a symbol
    pub fn read_latest(&self, symbol_id: SymbolId) -> Option<Record> {
        self.buffers.get(&symbol_id).and_then(|buffer| {
            unsafe { buffer.read() }
        })
    }

    /// Returns true if buffer for symbol is empty
    pub fn is_empty(&self, symbol_id: SymbolId) -> bool {
        self.buffers.get(&symbol_id)
            .map(|buffer| buffer.is_empty())
            .unwrap_or(true)
    }

    /// Returns true if buffer for symbol is full
    pub fn is_full(&self, symbol_id: SymbolId) -> bool {
        self.buffers.get(&symbol_id)
            .map(|buffer| buffer.is_full())
            .unwrap_or(false)
    }

    /// Returns the number of symbols currently tracked
    pub fn num_symbols(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database() {
        let db = Database::default();

        // Test writing and reading
        assert!(db.write(1, 100.0, 1000));
        let record = db.read_latest(1).unwrap();
        assert_eq!(record.symbol_id, 1);
        assert_eq!(record.price.as_f64(), 100.0);
        assert_eq!(record.quantity, 1000);

        // Test multiple symbols
        assert!(db.write(2, 200.0, 2000));
        let record = db.read_latest(2).unwrap();
        assert_eq!(record.symbol_id, 2);
        assert_eq!(record.price.as_f64(), 200.0);
        assert_eq!(record.quantity, 2000);

        assert_eq!(db.num_symbols(), 2);
    }
} 