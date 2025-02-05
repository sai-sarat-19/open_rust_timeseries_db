use std::sync::Arc;
use crate::core::config::UltraLowLatencyRecord;
use crate::core::index::{SymbolIndex, SymbolMetadata};
use crate::memory::zero_alloc_ring_buffer::ZeroAllocRingBuffer;

/// A thread-safe, ultra-low-latency database.
/// Can be either owned or referenced, with methods supporting both patterns.
#[derive(Clone)]
pub struct UltraLowLatencyDB<T: UltraLowLatencyRecord> {
    ring: Arc<ZeroAllocRingBuffer<T>>,
    symbol_index: Arc<SymbolIndex>,
}

impl<T: UltraLowLatencyRecord> UltraLowLatencyDB<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: Arc::new(ZeroAllocRingBuffer::new(capacity)),
            symbol_index: Arc::new(SymbolIndex::new()),
        }
    }

    /// Register a new symbol with metadata
    pub fn register_symbol(&self, symbol_id: u32, metadata: SymbolMetadata) {
        self.symbol_index.register_symbol(symbol_id, metadata);
    }

    /// Write a record into the DB. Returns false if the ring is full.
    /// This method can be called on either an owned DB or a reference.
    #[inline(always)]
    pub fn write(&self, record: &T) -> bool {
        let success = unsafe { self.ring.write(record) };
        if success {
            // Update symbol index with the current write position
            if let Some(symbol_id) = record.get_symbol_id() {
                self.symbol_index.update_position(symbol_id, self.ring.write_position());
            }
        }
        success
    }

    /// Read a record from the DB. Returns a copy of the record if available.
    /// This method can be called on either an owned DB or a reference.
    #[inline(always)]
    pub fn read(&self) -> Option<T> {
        unsafe { self.ring.read().map(|r| *r) }
    }

    /// Read the latest record for a specific symbol
    #[inline(always)]
    pub fn read_symbol(&self, symbol_id: u32) -> Option<T> {
        if let Some(position) = self.symbol_index.get_position(symbol_id) {
            unsafe { self.ring.read_at(position) }
        } else {
            None
        }
    }

    /// Get metadata for a symbol
    pub fn get_symbol_metadata(&self, symbol_id: u32) -> Option<SymbolMetadata> {
        self.symbol_index.get_metadata(symbol_id)
    }

    /// Get all registered symbols
    pub fn get_all_symbols(&self) -> Vec<u32> {
        self.symbol_index.get_all_symbols()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.ring.is_full()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    /// Get a reference to this DB that can be safely shared across threads
    #[inline(always)]
    pub fn as_ref(&self) -> &Self {
        self
    }
} 