pub mod feed {
    pub mod types;
    pub mod websocket;
    
    pub use types::{FeedMessage, FeedSource, MessageType, FeedStats};
    pub use websocket::WebSocketHandler;
}

pub mod store {
    pub mod global_market_data;
    pub mod redis_manager;
    
    pub use global_market_data::{GlobalMarketData, GlobalConfig, MarketDataStats, MarketDataError};
    pub use redis_manager::{RedisManager, RedisStats};
}

pub mod timeseries {
    pub mod manager;
    
    pub use manager::{TimeSeriesManager, TimeSeriesConfig, CompressionLevel, TimeSeriesStats};
}

// Re-export key types for convenience
pub use feed::{FeedMessage, FeedSource, MessageType, WebSocketHandler};
pub use store::{GlobalMarketData, GlobalConfig, RedisManager};
pub use timeseries::{TimeSeriesManager, TimeSeriesConfig, CompressionLevel};

// Error types
pub use anyhow::Result;
pub use thiserror::Error;

// Logging
pub use tracing;

// Buffer configuration type
#[derive(Debug, Clone)]
pub struct InstrumentBufferConfig {
    pub l1_buffer_size: usize,
    pub l2_buffer_size: usize,
    pub ref_buffer_size: usize,
}

/// Initialize the market data system with default configuration
pub async fn init() -> Result<(GlobalMarketData, RedisManager, TimeSeriesManager)> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Create global market data store
    let config = store::GlobalConfig {
        num_instruments: 10_000,
        cache_size_mb: 1024,
        num_threads: num_cpus::get(),
        buffer_config: InstrumentBufferConfig {
            l1_buffer_size: 1_048_576,  // 1M
            l2_buffer_size: 524_288,    // 512K
            ref_buffer_size: 65_536,    // 64K
        },
    };
    
    let market_data = GlobalMarketData::new(config)?;
    let redis = RedisManager::new("redis://localhost:6379")?;
    let time_series = TimeSeriesManager::new()?;
    
    // Start background processing
    market_data.start_background_processing()?;
    
    Ok((market_data, redis, time_series))
}

/// Initialize the market data system with custom configuration
pub async fn init_with_config(
    market_data_config: store::GlobalConfig,
    redis_url: &str,
    time_series_config: timeseries::TimeSeriesConfig,
) -> Result<(GlobalMarketData, RedisManager, TimeSeriesManager)> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    let market_data = GlobalMarketData::new(market_data_config)?;
    let redis = RedisManager::new(redis_url)?;
    let time_series = TimeSeriesManager::new()?;
    
    // Start background processing
    market_data.start_background_processing()?;
    
    Ok((market_data, redis, time_series))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    
    pub mod integration_test;
    pub use integration_test::test_full_system_integration;
} 