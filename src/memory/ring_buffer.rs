use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::mem::MaybeUninit;
use std::ptr;
use std::hint::spin_loop;

use crate::core::record::Record;

/// Ultra-low-latency lock-free ring buffer optimized for HFT
#[repr(align(64))]
pub struct RingBuffer {
    /// Pre-allocated buffer for records
    buffer: Box<[MaybeUninit<Record>]>,
    /// Buffer capacity (must be power of 2)
    capacity_mask: usize,
    /// Write index
    write_idx: AtomicU64,
    /// Read index
    read_idx: AtomicU64,
    /// Cache line padding
    _pad: [u8; 40],
}

impl RingBuffer {
    /// Creates a new ring buffer with the given capacity (rounded up to next power of 2)
    pub fn new(capacity: usize) -> Self {
        // Round up to power of 2
        let capacity = capacity.next_power_of_two();
        let mut v = Vec::with_capacity(capacity);
        v.resize_with(capacity, || MaybeUninit::uninit());
        
        Self {
            buffer: v.into_boxed_slice(),
            capacity_mask: capacity - 1,
            write_idx: AtomicU64::new(0),
            read_idx: AtomicU64::new(0),
            _pad: [0; 40],
        }
    }

    /// Attempts to write a record to the buffer
    /// Returns true if successful, false if buffer is full
    #[inline(always)]
    pub unsafe fn write(&self, record: &Record) -> bool {
        let idx = self.write_idx.load(Ordering::Relaxed) as usize;
        let next_idx = (idx + 1) & self.capacity_mask;

        // Check if buffer is full
        if next_idx == (self.read_idx.load(Ordering::Relaxed) as usize) {
            return false;
        }

        // Write record using appropriate method based on architecture
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                use std::arch::x86_64::{_mm256_stream_si256, __m256i, _mm256_load_si256};
                let src = record as *const Record as *const __m256i;
                let dst = self.buffer.as_ptr().add(idx) as *mut __m256i;
                _mm256_stream_si256(dst, _mm256_load_si256(src));
            } else {
                ptr::copy_nonoverlapping(
                    record as *const Record,
                    self.buffer.as_ptr().add(idx) as *mut Record,
                    1
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            ptr::copy_nonoverlapping(
                record as *const Record,
                self.buffer.as_ptr().add(idx) as *mut Record,
                1
            );
        }

        fence(Ordering::Release);
        self.write_idx.store(next_idx as u64, Ordering::Release);
        true
    }

    /// Attempts to read a record from the buffer
    /// Returns None if buffer is empty
    #[inline(always)]
    pub unsafe fn read(&self) -> Option<Record> {
        let idx = self.read_idx.load(Ordering::Relaxed) as usize;
        
        // Check if buffer is empty
        if idx == (self.write_idx.load(Ordering::Relaxed) as usize) {
            return None;
        }

        // Read record
        let record = ptr::read(self.buffer.as_ptr().add(idx) as *const Record);
        
        let next_idx = (idx + 1) & self.capacity_mask;
        self.read_idx.store(next_idx as u64, Ordering::Release);
        
        Some(record)
    }

    /// Returns true if buffer is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        (self.read_idx.load(Ordering::Relaxed) as usize) == 
        (self.write_idx.load(Ordering::Relaxed) as usize)
    }

    /// Returns true if buffer is full
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let next_write = ((self.write_idx.load(Ordering::Relaxed) as usize) + 1) 
            & self.capacity_mask;
        next_write == (self.read_idx.load(Ordering::Relaxed) as usize)
    }

    /// Returns current capacity of the buffer
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity_mask + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::*;

    #[test]
    fn test_ring_buffer() {
        let ring = RingBuffer::new(4);
        
        // Test empty buffer
        assert!(ring.is_empty());
        assert!(!ring.is_full());
        unsafe { assert!(ring.read().is_none()) };

        // Test writing and reading
        let record = Record::with_current_time(1, 100, 1234.56, 1000, 0);
        unsafe {
            assert!(ring.write(&record));
            let read = ring.read().unwrap();
            assert_eq!(read.id, record.id);
            assert_eq!(read.price.as_f64(), record.price.as_f64());
        }

        // Test buffer full condition
        unsafe {
            for i in 0..3 {
                assert!(ring.write(&Record::with_current_time(i, 100, 1000.0, 1000, 0)));
            }
            assert!(ring.is_full());
            assert!(!ring.write(&record));
        }
    }
} 