/// CPU utilities for performance optimization
pub mod cpu {
    use std::time::Instant;

    /// Measures CPU cycles using rdtsc
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub unsafe fn rdtsc_serialized() -> u64 {
        core::arch::x86_64::_mm_mfence();
        core::arch::x86_64::_mm_lfence();
        let tsc = core::arch::x86_64::_rdtsc();
        core::arch::x86_64::_mm_lfence();
        tsc
    }

    /// Measures CPU cycles (fallback for non-x86_64)
    #[cfg(not(target_arch = "x86_64"))]
    #[inline(always)]
    pub unsafe fn rdtsc_serialized() -> u64 {
        let now = Instant::now();
        now.elapsed().as_nanos() as u64
    }
}

/// Memory utilities
pub mod memory {
    /// Ensures memory is aligned to cache line boundary
    #[inline(always)]
    pub fn is_cache_aligned<T>(ptr: *const T) -> bool {
        (ptr as usize) % 64 == 0
    }
} 