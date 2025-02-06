use std::time::Duration;

/// Configuration for buffer sizes and latency thresholds
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Size of the ring buffer in number of records
    pub buffer_size: usize,
    /// Maximum acceptable write latency
    pub max_write_latency: Duration,
    /// Maximum acceptable read latency
    pub max_read_latency: Duration,
    /// Whether to use SIMD operations when available
    pub use_simd: bool,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            buffer_size: 16384,  // 16K records default
            max_write_latency: Duration::from_nanos(100),  // 100ns max write latency
            max_read_latency: Duration::from_nanos(50),    // 50ns max read latency
            use_simd: true,
        }
    }
}

/// Trait for custom record types that can be stored in the ultra-low-latency database
pub trait UltraLowLatencyRecord: Clone + Copy + Send + Sync + 'static {
    /// Get the size of the record in bytes
    fn size_bytes() -> usize;
    
    /// Get the alignment requirement for the record
    fn alignment() -> usize;
    
    /// Validate the record before writing
    fn validate(&self) -> bool;
    
    /// Convert to bytes for writing
    unsafe fn to_bytes(&self) -> &[u8];
    
    /// Create from bytes when reading
    unsafe fn from_bytes(bytes: &[u8]) -> Self;

    /// Get the instrument/symbol identifier
    fn symbol_id(&self) -> u32;
} 