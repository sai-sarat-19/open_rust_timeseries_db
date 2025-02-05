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

    fn get_token(&self) -> u64;
    fn get_timestamp(&self) -> u64;
    fn get_sequence_num(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct InstrumentBufferConfig {
    pub l1_buffer_size: usize,
    pub l2_buffer_size: usize,
    pub ref_buffer_size: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum BufferType {
    L1Price,
    L2Trade,
    Reference,
}

pub struct LatencyStats {
    pub min: u64,
    pub max: u64,
    pub median: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
}

pub fn calculate_latency_stats(latencies: &[u64]) -> LatencyStats {
    if latencies.is_empty() {
        return LatencyStats {
            min: 0,
            max: 0,
            median: 0,
            p90: 0,
            p99: 0,
            p999: 0,
        };
    }

    let len = latencies.len();
    LatencyStats {
        min: latencies[0],
        max: latencies[len - 1],
        median: latencies[len / 2],
        p90: latencies[(len as f64 * 0.90) as usize],
        p99: latencies[(len as f64 * 0.99) as usize],
        p999: latencies[(len as f64 * 0.999) as usize],
    }
}

pub fn print_latency_stats(stats: &LatencyStats) {
    println!("  Min: {} cycles", stats.min);
    println!("  Max: {} cycles", stats.max);
    println!("  Median: {} cycles", stats.median);
    println!("  p90: {} cycles", stats.p90);
    println!("  p99: {} cycles", stats.p99);
    println!("  p99.9: {} cycles", stats.p999);
}

#[inline(always)]
pub fn cycles_to_nanos(cycles: u64) -> u64 {
    // Assuming a 3GHz processor
    cycles * 1_000_000_000 / 3_000_000_000
} 