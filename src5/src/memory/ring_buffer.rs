use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::ptr;
use std::mem::MaybeUninit;

use crate::core::record::UltraLowLatencyRecord;

/// Zero-allocation ring buffer optimized for ultra-low latency
#[repr(align(64))]
pub struct ZeroAllocRingBuffer<T: UltraLowLatencyRecord> {
    data: Box<[MaybeUninit<T>]>,
    capacity: usize,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    last_sequence: AtomicU64,
    _pad: [u8; 32],  // Padding to prevent false sharing
}

impl<T: UltraLowLatencyRecord> ZeroAllocRingBuffer<T> {
    /// Creates a new ring buffer with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        data.resize_with(capacity, MaybeUninit::uninit);
        
        Self {
            data: data.into_boxed_slice(),
            capacity,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            last_sequence: AtomicU64::new(0),
            _pad: [0; 32],
        }
    }

    /// Attempts to write a record to the buffer
    /// Returns true if successful, false if buffer is full or sequence number is invalid
    #[inline(always)]
    pub unsafe fn write(&self, record: &T) -> bool {
        // Validate sequence number
        let seq = record.get_sequence_num();
        if seq <= self.last_sequence.load(Ordering::Acquire) {
            return false;
        }

        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let next_write = (write_pos + 1) % self.capacity;

        // Check if buffer is full
        if next_write == self.read_pos.load(Ordering::Acquire) {
            return false;
        }

        // Validate record
        if !record.validate() {
            return false;
        }

        // Perform zero-copy write
        ptr::copy_nonoverlapping(
            record as *const T,
            self.data.as_ptr().add(write_pos) as *mut T,
            1
        );

        // Memory fence to ensure write is visible
        fence(Ordering::Release);
        
        // Update write position and sequence
        self.write_pos.store(next_write, Ordering::Release);
        self.last_sequence.store(seq, Ordering::Release);
        
        true
    }

    /// Attempts to read a record from the buffer
    /// Returns None if buffer is empty
    #[inline(always)]
    pub unsafe fn read(&self) -> Option<T> {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        
        // Check if buffer is empty
        if read_pos == self.write_pos.load(Ordering::Acquire) {
            return None;
        }

        // Perform zero-copy read
        let record = ptr::read(self.data.as_ptr().add(read_pos) as *const T);
        
        let next_read = (read_pos + 1) % self.capacity;
        self.read_pos.store(next_read, Ordering::Release);
        
        Some(record)
    }

    /// Returns true if the buffer is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.read_pos.load(Ordering::Relaxed) == self.write_pos.load(Ordering::Acquire)
    }

    /// Returns true if the buffer is full
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let next_write = (write_pos + 1) % self.capacity;
        next_write == self.read_pos.load(Ordering::Acquire)
    }

    /// Returns the current number of records in the buffer
    #[inline(always)]
    pub fn len(&self) -> usize {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = self.read_pos.load(Ordering::Acquire);
        
        if write_pos >= read_pos {
            write_pos - read_pos
        } else {
            self.capacity - (read_pos - write_pos)
        }
    }

    /// Returns the capacity of the buffer
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// Safety: The ring buffer is thread-safe due to atomic operations
unsafe impl<T: UltraLowLatencyRecord> Send for ZeroAllocRingBuffer<T> {}
unsafe impl<T: UltraLowLatencyRecord> Sync for ZeroAllocRingBuffer<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::MarketDataRecord;

    #[test]
    fn test_ring_buffer_basic_operations() {
        let buffer = ZeroAllocRingBuffer::<MarketDataRecord>::new(4);
        
        let record1 = MarketDataRecord::new(1, 100.0, 101.0, 100, 100, 100.5, 50, 1, 1000, 0);
        let record2 = MarketDataRecord::new(1, 100.1, 101.1, 100, 100, 100.6, 50, 2, 1001, 0);
        
        unsafe {
            assert!(buffer.write(&record1));
            assert!(buffer.write(&record2));
            
            let read1 = buffer.read().unwrap();
            assert_eq!(read1.sequence_num, 1);
            
            let read2 = buffer.read().unwrap();
            assert_eq!(read2.sequence_num, 2);
            
            assert!(buffer.read().is_none());
        }
    }

    #[test]
    fn test_ring_buffer_full() {
        let buffer = ZeroAllocRingBuffer::<MarketDataRecord>::new(2);
        
        let record1 = MarketDataRecord::new(1, 100.0, 101.0, 100, 100, 100.5, 50, 1, 1000, 0);
        let record2 = MarketDataRecord::new(1, 100.1, 101.1, 100, 100, 100.6, 50, 2, 1001, 0);
        let record3 = MarketDataRecord::new(1, 100.2, 101.2, 100, 100, 100.7, 50, 3, 1002, 0);
        
        unsafe {
            assert!(buffer.write(&record1));
            assert!(buffer.write(&record2));
            assert!(!buffer.write(&record3)); // Buffer should be full
            
            buffer.read().unwrap(); // Make space
            assert!(buffer.write(&record3)); // Now should succeed
        }
    }

    #[test]
    fn test_sequence_validation() {
        let buffer = ZeroAllocRingBuffer::<MarketDataRecord>::new(4);
        
        let record1 = MarketDataRecord::new(1, 100.0, 101.0, 100, 100, 100.5, 50, 2, 1000, 0);
        let record2 = MarketDataRecord::new(1, 100.1, 101.1, 100, 100, 100.6, 50, 1, 1001, 0);
        
        unsafe {
            assert!(buffer.write(&record1));
            assert!(!buffer.write(&record2)); // Should fail due to lower sequence number
        }
    }
} 