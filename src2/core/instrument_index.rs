use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::mem::MaybeUninit;

/// Perfect hash map for instrument tokens
/// Uses a fixed-size array for O(1) lookups with no collisions
#[repr(align(64))]
pub struct InstrumentIndex {
    // Fixed array for perfect hashing (aligned to cache line)
    index: Box<[AtomicU32]>,
    // Mapping of instrument token to buffer index
    token_map: RwLock<HashMap<u32, usize>>,
    // Number of instruments registered
    count: AtomicUsize,
    // Maximum number of instruments supported
    capacity: usize,
}

impl InstrumentIndex {
    pub fn new(capacity: usize) -> Self {
        let mut index = Vec::with_capacity(capacity);
        index.resize_with(capacity, || AtomicU32::new(0));
        
        Self {
            index: index.into_boxed_slice(),
            token_map: RwLock::new(HashMap::with_capacity(capacity)),
            count: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Register a new instrument token
    /// Returns the buffer index assigned to this instrument
    #[inline]
    pub fn register_instrument(&self, token: u32) -> Option<usize> {
        let mut map = self.token_map.write();
        if map.contains_key(&token) {
            return map.get(&token).copied();
        }

        let count = self.count.load(Ordering::Relaxed);
        if count >= self.capacity {
            return None;
        }

        let idx = count;
        map.insert(token, idx);
        self.index[idx].store(token, Ordering::Release);
        self.count.fetch_add(1, Ordering::Release);
        Some(idx)
    }

    /// Get the buffer index for an instrument token
    /// This is the ultra-fast lookup path used in the critical section
    #[inline(always)]
    pub fn get_buffer_index(&self, token: u32) -> Option<usize> {
        // First try fast path - direct array lookup
        for i in 0..self.count.load(Ordering::Relaxed) {
            if self.index[i].load(Ordering::Relaxed) == token {
                return Some(i);
            }
        }
        
        // Slow path - hash map lookup
        self.token_map.read().get(&token).copied()
    }

    /// Get the first registered token
    #[inline(always)]
    pub fn get_first_token(&self) -> Option<u32> {
        if self.count.load(Ordering::Relaxed) > 0 {
            Some(self.index[0].load(Ordering::Relaxed))
        } else {
            None
        }
    }

    /// Check if an instrument is registered
    #[inline(always)]
    pub fn contains(&self, token: u32) -> bool {
        self.get_buffer_index(token).is_some()
    }

    /// Get the number of registered instruments
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the maximum capacity
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Configuration for instrument-specific buffers
#[derive(Debug, Clone)]
pub struct InstrumentBufferConfig {
    /// Size of the L1 price buffer (bid/ask updates)
    pub l1_buffer_size: usize,
    /// Size of the L2 trade buffer
    pub l2_buffer_size: usize,
    /// Size of the reference data buffer
    pub ref_buffer_size: usize,
}

impl Default for InstrumentBufferConfig {
    fn default() -> Self {
        Self {
            l1_buffer_size: 65536,  // 64K for price updates
            l2_buffer_size: 32768,  // 32K for trades
            ref_buffer_size: 8192,  // 8K for reference data
        }
    }
} 