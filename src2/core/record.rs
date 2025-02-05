#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct UltraLowLatencyRecord {
    pub symbol_id: u32,   // 4 bytes
    pub price: f64,       // 8 bytes
    pub quantity: u32,    // 4 bytes
    pub timestamp: u64,   // 8 bytes
    pub flags: u8,        // 1 byte
    pub _padding: [u8; 39], // Pad to 64 bytes (cache line)
}

impl UltraLowLatencyRecord {
    pub fn new(symbol_id: u32, price: f64, quantity: u32, timestamp: u64, flags: u8) -> Self {
        Self {
            symbol_id,
            price,
            quantity,
            timestamp,
            flags,
            _padding: [0; 39],
        }
    }

    #[inline(always)]
    pub fn update_price(&mut self, new_price: f64) {
        self.price = new_price;
    }

    #[inline(always)]
    pub fn update_quantity(&mut self, new_quantity: u32) {
        self.quantity = new_quantity;
    }
} 