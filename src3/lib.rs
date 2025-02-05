pub mod core {
    pub mod config;
    pub mod record;
    pub mod instrument_index;
}

pub mod memory {
    pub mod instrument_buffer;
    pub mod zero_alloc_ring_buffer;
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use std::sync::atomic::{AtomicBool, Ordering};
    use crate::core::record::MarketDataRecord;
    use crate::core::config::{InstrumentBufferConfig, BufferType, calculate_latency_stats, print_latency_stats};
    use crate::memory::instrument_buffer::InstrumentBufferManager;

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    unsafe fn rdtsc_serialized() -> u64 {
        use core::arch::x86_64::{_mm_lfence, _rdtsc};
        _mm_lfence();
        let cycles = _rdtsc();
        _mm_lfence();
        cycles
    }

    pub mod basic;
    pub mod production;
} 