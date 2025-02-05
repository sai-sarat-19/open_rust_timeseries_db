use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::mem::MaybeUninit;
use std::ptr;
use std::hint::spin_loop;

use crate::core::config::UltraLowLatencyRecord;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_mm256_stream_si256, __m256i, _mm256_load_si256};

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

        // Use SIMD streaming if available on x86_64 and record size is appropriate
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && T::size_bytes() >= 32 {
                let src = record as *const T as *const __m256i;
                let dst = self.buffer.as_ptr().add(idx) as *mut __m256i;
                _mm256_stream_si256(dst, _mm256_load_si256(src));
            } else {
                ptr::copy_nonoverlapping(
                    record as *const T,
                    self.buffer.as_ptr().add(idx) as *mut T,
                    1
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            ptr::copy_nonoverlapping(
                record as *const T,
                self.buffer.as_ptr().add(idx) as *mut T,
                1
            );
        }
        
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

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.read_idx.load(Ordering::Relaxed) == self.write_idx.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let next = (self.write_idx.load(Ordering::Relaxed) + 1) % self.capacity as u64;
        next == self.read_idx.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
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