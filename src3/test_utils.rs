#[derive(Debug)]
pub struct LatencyStats {
    pub min: u64,
    pub max: u64,
    pub median: u64,
    pub p90: u64,
    pub p99: u64,
    pub p99_9: u64,
}

pub fn calculate_latency_stats(latencies: &[u64]) -> LatencyStats {
    let len = latencies.len();
    LatencyStats {
        min: latencies[0],
        max: *latencies.last().unwrap(),
        median: latencies[len / 2],
        p90: latencies[(len as f64 * 0.90) as usize],
        p99: latencies[(len as f64 * 0.99) as usize],
        p99_9: latencies[(len as f64 * 0.999) as usize],
    }
}

pub fn print_latency_stats(stats: &LatencyStats) {
    println!("  Min: {} cycles ({} ns)", stats.min, cycles_to_nanos(stats.min));
    println!("  Median: {} cycles ({} ns)", stats.median, cycles_to_nanos(stats.median));
    println!("  P90: {} cycles ({} ns)", stats.p90, cycles_to_nanos(stats.p90));
    println!("  P99: {} cycles ({} ns)", stats.p99, cycles_to_nanos(stats.p99));
    println!("  P99.9: {} cycles ({} ns)", stats.p99_9, cycles_to_nanos(stats.p99_9));
    println!("  Max: {} cycles ({} ns)", stats.max, cycles_to_nanos(stats.max));
}

pub fn cycles_to_nanos(cycles: u64) -> u64 {
    cycles * 1_000_000_000 / 3_000_000_000
} 