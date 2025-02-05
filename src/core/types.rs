use std::time::{SystemTime, UNIX_EPOCH};

/// High-precision timestamp type optimized for HFT
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Creates a new timestamp from nanoseconds
    #[inline(always)]
    pub fn new(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Gets current timestamp with nanosecond precision
    #[inline(always)]
    pub fn now() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self(nanos)
    }

    /// Gets raw nanosecond value
    #[inline(always)]
    pub fn as_nanos(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for records
pub type RecordId = u64;

/// Price type with fixed decimal precision
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub struct Price(i64);

impl Price {
    /// Number of decimal places for price values
    pub const DECIMALS: u32 = 8;
    const MULTIPLIER: i64 = 10i64.pow(Self::DECIMALS);

    /// Creates a new price from a decimal number
    #[inline(always)]
    pub fn new(value: f64) -> Self {
        Self((value * Self::MULTIPLIER as f64) as i64)
    }

    /// Gets the raw fixed-point value
    #[inline(always)]
    pub fn raw_value(&self) -> i64 {
        self.0
    }

    /// Converts to f64
    #[inline(always)]
    pub fn as_f64(&self) -> f64 {
        self.0 as f64 / Self::MULTIPLIER as f64
    }
}

/// Quantity type for order sizes
pub type Quantity = u32;

/// Symbol identifier type
pub type SymbolId = u32;

/// Flags for record status and properties
pub type Flags = u8; 