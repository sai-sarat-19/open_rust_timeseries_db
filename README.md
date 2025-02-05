# Ultra-Low-Latency Database

An ultra-low-latency in-memory database optimized for high-frequency trading (HFT) applications. This database is designed for maximum performance with minimal latency, using lock-free data structures and zero-allocation writes.

## Features

- **Ultra-Low Latency**: Optimized for minimal latency using lock-free ring buffers
- **Zero-Allocation Writes**: No memory allocation during writes
- **Cache-Friendly**: Cache-line aligned data structures
- **SIMD Optimizations**: Uses AVX2 instructions when available
- **Symbol Partitioning**: Data partitioned by symbol for parallel access
- **Thread-Safe**: Lock-free concurrent access
- **Minimal Dependencies**: Only essential dependencies for maximum performance

## Architecture

The database is built around these core components:

1. **Record**: A fixed-size, cache-line aligned (64-byte) struct containing:
   - Record ID
   - Symbol ID
   - Price (fixed-point)
   - Quantity
   - Timestamp
   - Flags

2. **Ring Buffer**: A lock-free, zero-allocation ring buffer optimized for:
   - Single-producer/single-consumer usage per symbol
   - Direct memory writes using SIMD instructions
   - Cache-line alignment for optimal performance

3. **Database Engine**: Manages:
   - Symbol-partitioned ring buffers
   - Record ID generation
   - Buffer allocation and access

## Performance

The database is optimized for:

- Write latency: < 100ns (99th percentile)
- Read latency: < 50ns (99th percentile)
- Zero garbage collection
- Zero memory allocation during normal operation
- Minimal cache misses

## Usage

```rust
use ultra_low_latency_db::{Database, DatabaseConfig};

// Create database with default configuration
let db = Database::default();

// Write a record
db.write(symbol_id: 1, price: 100.0, quantity: 1000);

// Read latest record for symbol
if let Some(record) = db.read_latest(symbol_id: 1) {
    println!("Latest price: {}", record.price.as_f64());
}
```

## Configuration

The database can be configured with custom settings:

```rust
let config = DatabaseConfig {
    buffer_capacity: 16384,  // 16K records per buffer
    num_partitions: 64,      // 64 symbol partitions
};
let db = Database::new(config);
```

## Building and Testing

```bash
# Build
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Performance Tips

1. **CPU Affinity**: Pin your threads to specific CPU cores
2. **Huge Pages**: Use huge pages for the ring buffers
3. **CPU Frequency**: Disable CPU frequency scaling
4. **IRQ Affinity**: Route interrupts away from your HFT cores
5. **NUMA**: Ensure memory allocation on the correct NUMA node

## Limitations

- In-memory only (no persistence)
- Fixed record size
- No complex queries
- No transaction support
- Limited to power-of-2 buffer sizes

## Contributing

Contributions are welcome! Please ensure your changes maintain the ultra-low-latency characteristics of the database.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

This project is inspired by various HFT systems and lock-free data structures, particularly:
- LMAX Disruptor
- Aeron
- Chronicle Queue