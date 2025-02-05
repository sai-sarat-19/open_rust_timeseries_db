use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering, fence};
use std::mem::MaybeUninit;

// Constants for performance tuning
const CACHE_LINE_SIZE: usize = 64;
const SPIN_LIMIT: u32 = 6;  // Optimal spin count before yielding

// Cache-line aligned slot for better performance
#[repr(align(64))]
struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,  // Use MaybeUninit for better performance
    _padding: [u8; CACHE_LINE_SIZE - 16],  // Prevent false sharing
}

// Thread safety implementations
unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

// Cache-line aligned ring buffer
#[repr(align(64))]
pub struct LowLatencyMpmcRing<T> {
    buffer: Box<[Slot<T>]>,
    capacity: usize,
    mask: usize,
    producer_index: AtomicUsize,
    consumer_index: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - 40],  // Prevent false sharing
}

// Thread safety implementations
unsafe impl<T: Send> Send for LowLatencyMpmcRing<T> {}
unsafe impl<T: Send> Sync for LowLatencyMpmcRing<T> {}

impl<T> LowLatencyMpmcRing<T> {
    #[inline(always)]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        
        // Pre-allocate all slots
        let mut vec = Vec::with_capacity(capacity);
        for i in 0..capacity {
            vec.push(Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
                _padding: [0; CACHE_LINE_SIZE - 16],
            });
        }

        Self {
            buffer: vec.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            producer_index: AtomicUsize::new(0),
            consumer_index: AtomicUsize::new(0),
            _padding: [0; CACHE_LINE_SIZE - 40],
        }
    }

    #[inline(always)]
    pub fn try_enqueue(&self, item: T) -> bool {
        let mut spin_count = 0;
        loop {
            let seq = self.producer_index.load(Ordering::Acquire);
            let idx = seq & self.mask;

            // Fast path: check sequence without acquiring the slot
            let slot = unsafe { self.buffer.get_unchecked(idx) };
            let slot_seq = slot.sequence.load(Ordering::Acquire);

            if slot_seq == seq {
                if self.producer_index.compare_exchange_weak(
                    seq, seq.wrapping_add(1),
                    Ordering::AcqRel, Ordering::Relaxed
                ).is_ok() {
                    // Write the value
                    unsafe {
                        (*slot.value.get()).write(item);
                    }
                    fence(Ordering::Release);
                    slot.sequence.store(seq.wrapping_add(1), Ordering::Release);
                    return true;
                }
            } else if slot_seq < seq {
                return false;
            } else {
                spin_count += 1;
                if spin_count > SPIN_LIMIT {
                    std::thread::yield_now();
                    spin_count = 0;
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub fn try_dequeue(&self) -> Option<T> {
        let mut spin_count = 0;
        loop {
            let seq = self.consumer_index.load(Ordering::Acquire);
            let idx = seq & self.mask;

            // Fast path: check sequence without acquiring the slot
            let slot = unsafe { self.buffer.get_unchecked(idx) };
            let slot_seq = slot.sequence.load(Ordering::Acquire);

            if slot_seq == seq.wrapping_add(1) {
                if self.consumer_index.compare_exchange_weak(
                    seq, seq.wrapping_add(1),
                    Ordering::AcqRel, Ordering::Relaxed
                ).is_ok() {
                    // Take ownership of the value
                    let val = unsafe {
                        (*slot.value.get()).assume_init_read()
                    };
                    fence(Ordering::Release);
                    slot.sequence.store(seq.wrapping_add(self.capacity), Ordering::Release);
                    return Some(val);
                }
            } else if slot_seq < seq.wrapping_add(1) {
                return None;
            } else {
                spin_count += 1;
                if spin_count > SPIN_LIMIT {
                    std::thread::yield_now();
                    spin_count = 0;
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.producer_index.load(Ordering::Relaxed) == self.consumer_index.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        (self.producer_index.load(Ordering::Relaxed) - 
         self.consumer_index.load(Ordering::Relaxed)) >= self.capacity
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
} 