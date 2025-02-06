use std::sync::atomic::{AtomicU64, Ordering};

/// Trait for ultra-low-latency records that can be stored in ring buffers
pub trait UltraLowLatencyRecord: Copy + Send + Sync {
    fn get_sequence_num(&self) -> u64;
    fn get_token(&self) -> u64;
    fn validate(&self) -> bool;
}

/// Market data record optimized for cache-line alignment and zero-copy operations
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct MarketDataRecord {
    pub token: u64,             // 8 bytes
    pub bid_price: f64,         // 8 bytes
    pub ask_price: f64,         // 8 bytes
    pub bid_size: u32,          // 4 bytes
    pub ask_size: u32,          // 4 bytes
    pub last_price: f64,        // 8 bytes
    pub last_size: u32,         // 4 bytes
    pub sequence_num: u64,      // 8 bytes
    pub timestamp: u64,         // 8 bytes
    pub flags: u8,              // 1 byte
    _padding: [u8; 3],          // 3 bytes padding to maintain alignment
}

impl MarketDataRecord {
    #[inline(always)]
    pub fn new(
        token: u64,
        bid_price: f64,
        ask_price: f64,
        bid_size: u32,
        ask_size: u32,
        last_price: f64,
        last_size: u32,
        sequence_num: u64,
        timestamp: u64,
        flags: u8,
    ) -> Self {
        Self {
            token,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            last_price,
            last_size,
            sequence_num,
            timestamp,
            flags,
            _padding: [0; 3],
        }
    }

    #[inline(always)]
    pub fn update_prices(&mut self, bid: f64, ask: f64, last: f64) {
        self.bid_price = bid;
        self.ask_price = ask;
        self.last_price = last;
    }

    #[inline(always)]
    pub fn update_sizes(&mut self, bid_size: u32, ask_size: u32, last_size: u32) {
        self.bid_size = bid_size;
        self.ask_size = ask_size;
        self.last_size = last_size;
    }
}

impl UltraLowLatencyRecord for MarketDataRecord {
    #[inline(always)]
    fn get_sequence_num(&self) -> u64 {
        self.sequence_num
    }

    #[inline(always)]
    fn get_token(&self) -> u64 {
        self.token
    }

    #[inline(always)]
    fn validate(&self) -> bool {
        self.token > 0 && self.sequence_num > 0 && self.timestamp > 0
    }
}

/// Statistics for tracking record operations
#[derive(Debug, Default)]
pub struct RecordStats {
    pub total_updates: AtomicU64,
    pub invalid_records: AtomicU64,
    pub sequence_errors: AtomicU64,
}

impl RecordStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn increment_updates(&self) {
        self.total_updates.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn increment_invalid(&self) {
        self.invalid_records.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn increment_sequence_errors(&self) {
        self.sequence_errors.fetch_add(1, Ordering::Relaxed);
    }
} 