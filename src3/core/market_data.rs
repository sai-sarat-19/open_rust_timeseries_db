use std::mem;
use super::config::UltraLowLatencyRecord;

/// Market data record optimized for HFT
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct MarketDataRecord {
    pub symbol_id: u32,       // 4 bytes
    pub bid_price: f64,       // 8 bytes
    pub ask_price: f64,       // 8 bytes
    pub bid_size: u32,        // 4 bytes
    pub ask_size: u32,        // 4 bytes
    pub last_price: f64,      // 8 bytes
    pub last_size: u32,       // 4 bytes
    pub timestamp: u64,       // 8 bytes
    pub sequence_num: u64,    // 8 bytes
    pub flags: u8,            // 1 byte
    _padding: [u8; 7],        // Pad to 64 bytes
}

impl MarketDataRecord {
    pub fn new(
        symbol_id: u32,
        bid_price: f64,
        ask_price: f64,
        bid_size: u32,
        ask_size: u32,
        last_price: f64,
        last_size: u32,
        timestamp: u64,
        sequence_num: u64,
        flags: u8,
    ) -> Self {
        Self {
            symbol_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            last_price,
            last_size,
            timestamp,
            sequence_num,
            flags,
            _padding: [0; 7],
        }
    }
}

impl UltraLowLatencyRecord for MarketDataRecord {
    fn size_bytes() -> usize {
        mem::size_of::<Self>()
    }

    fn alignment() -> usize {
        mem::align_of::<Self>()
    }

    fn validate(&self) -> bool {
        // Basic validation rules
        self.bid_price > 0.0 
            && self.ask_price > 0.0 
            && self.ask_price >= self.bid_price
            && self.bid_size > 0 
            && self.ask_size > 0
    }

    unsafe fn to_bytes(&self) -> &[u8] {
        std::slice::from_raw_parts(
            self as *const Self as *const u8,
            mem::size_of::<Self>()
        )
    }

    unsafe fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= mem::size_of::<Self>());
        *(bytes.as_ptr() as *const Self)
    }

    fn symbol_id(&self) -> u32 {
        self.symbol_id
    }
} 