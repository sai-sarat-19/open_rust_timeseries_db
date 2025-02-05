use crate::core::config::UltraLowLatencyRecord;

#[derive(Debug, Clone, Copy)]
pub struct MarketDataRecord {
    pub token: u64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: u32,
    pub ask_size: u32,
    pub last_price: f64,
    pub last_size: u32,
    pub timestamp: u64,
    pub sequence_num: u64,
    pub record_type: u8,
}

impl MarketDataRecord {
    pub fn new(
        token: u64,
        bid_price: f64,
        ask_price: f64,
        bid_size: u32,
        ask_size: u32,
        last_price: f64,
        last_size: u32,
        timestamp: u64,
        sequence_num: u64,
        record_type: u8,
    ) -> Self {
        Self {
            token,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            last_price,
            last_size,
            timestamp,
            sequence_num,
            record_type,
        }
    }
}

unsafe impl Send for MarketDataRecord {}
unsafe impl Sync for MarketDataRecord {}

impl UltraLowLatencyRecord for MarketDataRecord {
    fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    fn alignment() -> usize {
        std::mem::align_of::<Self>()
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
            std::mem::size_of::<Self>()
        )
    }

    unsafe fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        *(bytes.as_ptr() as *const Self)
    }

    fn symbol_id(&self) -> u32 {
        self.token as u32
    }

    fn get_token(&self) -> u64 {
        self.token
    }

    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    fn get_sequence_num(&self) -> u64 {
        self.sequence_num
    }
} 