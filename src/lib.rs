// Ultra-low-latency database implementation
// Core modules
pub mod core;
pub mod memory;
pub mod storage;
pub mod engine;
pub mod utils;

// Re-exports of common types
pub use crate::core::types::Timestamp;
pub use crate::core::record::Record;
pub use crate::engine::db::Database;

// Version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests; 