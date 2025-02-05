# Ultra-Low-Latency Database

A high-performance, zero-allocation in-memory database designed for ultra-low-latency applications like High-Frequency Trading (HFT).

## Features

- **Ultra-Low Latency**: Optimized for minimal latency using cache-aligned structures and zero-allocation writes
- **SIMD Optimization**: Uses AVX2 instructions when available for faster memory operations
- **Lock-Free Design**: Uses atomic operations for thread-safe access without locks
- **Zero-Allocation**: Pre-allocated ring buffer eliminates allocation during operation
- **Cache-Friendly**: Cache-line aligned data structures for optimal performance
- **Precise Timing**: Hardware timestamp counter (TSC) for accurate latency measurements

## Architecture

The database consists of three main components:

1. **Core Record** (`core/record.rs`)
   - Cache-aligned fixed-size record structure
   - Optimized for HFT data (symbol, price, quantity, timestamp)

2. **Zero-Allocation Ring Buffer** (`memory/zero_alloc_ring_buffer.rs`)
   - Lock-free circular buffer implementation
   - Direct memory operations with optional SIMD
   - Pre-allocated memory to avoid runtime allocations

3. **Database Interface** (`db/ultra_low_latency_db.rs`)
   - Simple API for writing and reading records
   - Thread-safe operations through atomic operations

## Usage

```rust
use ultra_low_latency_db::{UltraLowLatencyDB, UltraLowLatencyRecord};

// Create a new DB instance with 16K capacity
let db = UltraLowLatencyDB::new(16384);

// Create a record
let record = UltraLowLatencyRecord::new(
    101,        // symbol_id
    10_000.0,   // price
    100,        // quantity
    0,          // timestamp
    0,          // flags
);

// Write record
if db.write(&record) {
    println!("Write successful");
}

// Read record
if let Some(read_record) = db.read() {
    println!("Read price: {}", read_record.price);
}
```

## Performance

The database is optimized for:
- Sub-microsecond latencies
- Zero allocations during operation
- Minimal cache misses
- Efficient multi-threaded access

Typical performance metrics (on modern hardware):
- Write latency: < 100ns (P99)
- Read latency: < 50ns (P99)
- Zero allocation overhead
- Cache-line optimized access

## Building and Testing

```bash
# Build in release mode
cargo build --release

# Run tests including latency benchmarks
cargo test

# Run the example
cargo run --release
```

## Implementation Details

### Cache-Line Alignment

All critical structures are aligned to 64-byte cache lines to prevent false sharing:

```rust
#[repr(C, align(64))]
pub struct UltraLowLatencyRecord {
    // Fields aligned for optimal memory access
}
```

### Zero-Allocation Ring Buffer

The ring buffer pre-allocates memory and uses atomic operations for thread-safe access:

```rust
pub struct ZeroAllocRingBuffer {
    buffer: Box<[MaybeUninit<UltraLowLatencyRecord>]>,
    write_idx: AtomicU64,
    read_idx: AtomicU64,
}
```

### SIMD Optimization

When available, AVX2 instructions are used for faster memory operations:

```rust
#[cfg(target_arch = "x86_64")]
if is_x86_feature_detected!("avx2") {
    _mm256_stream_si256(dst, _mm256_load_si256(src));
}
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 