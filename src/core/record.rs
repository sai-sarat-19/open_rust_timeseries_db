use crate::core::types::*;

/// Cache-line aligned record for ultra-low-latency storage
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct Record {
    /// Unique record identifier
    pub id: RecordId,
    /// Symbol identifier
    pub symbol_id: SymbolId,
    /// Price value
    pub price: Price,
    /// Quantity value
    pub quantity: Quantity,
    /// Timestamp in nanoseconds
    pub timestamp: Timestamp,
    /// Status flags
    pub flags: Flags,
    /// Padding to ensure 64-byte alignment
    _padding: [u8; 31],
}

impl Record {
    /// Creates a new record with the given values
    #[inline(always)]
    pub fn new(
        id: RecordId,
        symbol_id: SymbolId,
        price: f64,
        quantity: Quantity,
        timestamp: Timestamp,
        flags: Flags,
    ) -> Self {
        Self {
            id,
            symbol_id,
            price: Price::new(price),
            quantity,
            timestamp,
            flags,
            _padding: [0; 31],
        }
    }

    /// Creates a new record with current timestamp
    #[inline(always)]
    pub fn with_current_time(
        id: RecordId,
        symbol_id: SymbolId,
        price: f64,
        quantity: Quantity,
        flags: Flags,
    ) -> Self {
        Self::new(id, symbol_id, price, quantity, Timestamp::now(), flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_size() {
        assert_eq!(std::mem::size_of::<Record>(), 64);
        assert_eq!(std::mem::align_of::<Record>(), 64);
    }

    #[test]
    fn test_record_creation() {
        let record = Record::with_current_time(1, 100, 1234.56, 1000, 0);
        assert_eq!(record.id, 1);
        assert_eq!(record.symbol_id, 100);
        assert_eq!(record.price.as_f64(), 1234.56);
        assert_eq!(record.quantity, 1000);
        assert_eq!(record.flags, 0);
    }
} 