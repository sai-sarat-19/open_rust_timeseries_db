use std::sync::Arc;
use crate::core::record::UltraLowLatencyRecord;
use crate::memory::zero_alloc_ring_buffer::ZeroAllocRingBuffer;

/// A thread-safe, ultra-low-latency database.
/// Can be either owned or referenced, with methods supporting both patterns.
#[derive(Clone)]
pub struct UltraLowLatencyDB {
    ring: Arc<ZeroAllocRingBuffer>,
}

impl UltraLowLatencyDB {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: Arc::new(ZeroAllocRingBuffer::new(capacity))
        }
    }

    /// Write a record into the DB. Returns false if the ring is full.
    /// This method can be called on either an owned DB or a reference.
    #[inline(always)]
    pub fn write(&self, record: &UltraLowLatencyRecord) -> bool {
        unsafe { self.ring.write(record) }
    }

    /// Read a record from the DB. Returns a copy of the record if available.
    /// This method can be called on either an owned DB or a reference.
    #[inline(always)]
    pub fn read(&self) -> Option<UltraLowLatencyRecord> {
        unsafe { self.ring.read().map(|r| *r) }
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