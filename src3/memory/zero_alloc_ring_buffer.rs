use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::mem::MaybeUninit;
use std::ptr;

use crate::core::config::UltraLowLatencyRecord;

#[repr(align(64))]
pub struct ZeroAllocRingBuffer<T: UltraLowLatencyRecord> {
    buffer: Box<[MaybeUninit<T>]>,
    capacity: usize,
    write_idx: AtomicU64,
    read_idx: AtomicU64,
    _pad: [u8; 40],
}

impl<T: UltraLowLatencyRecord> ZeroAllocRingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let mut v = Vec::with_capacity(capacity);
        v.resize_with(capacity, || MaybeUninit::uninit());
        Self {
            buffer: v.into_boxed_slice(),
            capacity,
            write_idx: AtomicU64::new(0),
            read_idx: AtomicU64::new(0),
            _pad: [0; 40],
        }
    }

    /// Write a record directly into the ring buffer without allocation.
    /// Returns false if the ring is full.
    #[inline(always)]
    pub unsafe fn write(&self, record: &T) -> bool {
        let idx = self.write_idx.load(Ordering::Relaxed) as usize;
        let next_idx = (idx + 1) % self.capacity;
        
        // Check if buffer is full
        if next_idx == self.read_idx.load(Ordering::Relaxed) as usize {
            return false;
        }

        // Validate record before writing
        if !record.validate() {
            return false;
        }

        ptr::copy_nonoverlapping(
            record as *const T,
            self.buffer.as_ptr().add(idx) as *mut T,
            1
        );
        
        fence(Ordering::Release);
        self.write_idx.store(next_idx as u64, Ordering::Release);
        true
    }

    /// Read a record directly (zero-copy) from the ring buffer.
    /// Returns a reference to the record, or None if the ring is empty.
    #[inline(always)]
    pub unsafe fn read(&self) -> Option<&T> {
        let idx = self.read_idx.load(Ordering::Relaxed) as usize;
        if idx == self.write_idx.load(Ordering::Relaxed) as usize {
            return None;
        }
        
        // Safety: We know the buffer is not empty and idx is valid
        let ptr = self.buffer.as_ptr().add(idx);
        let record = &*(ptr as *const T);
        
        let next_idx = (idx + 1) % self.capacity;
        self.read_idx.store(next_idx as u64, Ordering::Release);
        Some(record)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub unsafe fn rdtsc_serialized() -> u64 {
    core::arch::x86_64::_mm_mfence();
    core::arch::x86_64::_mm_lfence();
    let tsc = core::arch::x86_64::_rdtsc();
    core::arch::x86_64::_mm_lfence();
    tsc
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub unsafe fn rdtsc_serialized() -> u64 {
    0
} 