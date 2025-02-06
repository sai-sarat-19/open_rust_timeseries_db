pub mod core;
pub mod memory;
pub mod feed;
pub mod store;
pub mod timeseries;

#[cfg(test)]
pub mod tests;

// Re-export key types
pub use core::{UltraLowLatencyRecord, MarketDataRecord, RecordStats};
pub use memory::ZeroAllocRingBuffer;

// Error types
pub use anyhow::Result;
pub use thiserror::Error;

// Logging
pub use tracing;

/// Initialize hardware-specific optimizations
#[inline(always)]
pub unsafe fn init_hardware_optimizations() {
    #[cfg(target_arch = "x86_64")]
    {
        // Ensure CPU is ready for low-latency operations
        std::arch::x86_64::_mm_mfence();
        std::arch::x86_64::_mm_lfence();
    }
}

/// Get high-precision timestamp
#[inline(always)]
pub unsafe fn rdtsc_timestamp() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_mm_lfence, _mm_mfence, _rdtsc};
        _mm_mfence();
        _mm_lfence();
        let tsc = _rdtsc();
        _mm_lfence();
        tsc
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
} 