pub mod feed {
    mod types;
    mod websocket;
    
    pub use types::{FeedMessage, FeedSource, MessageType, FeedStats};
    pub use websocket::WebSocketHandler;
}

pub mod store {
    mod global_market_data;
    
    pub use global_market_data::{GlobalMarketData, GlobalConfig, MarketDataStats, MarketDataError};
}

pub mod timeseries {
    mod manager;
    
    pub use manager::{TimeSeriesManager, TimeSeriesConfig, CompressionLevel, TimeSeriesStats};
}

// Re-export key types for convenience
pub use feed::{FeedMessage, WebSocketHandler};
pub use store::GlobalMarketData;
pub use timeseries::TimeSeriesManager;

// Error types
pub use anyhow::Result;
pub use thiserror::Error;

// Logging
pub use tracing;

/// Initialize the market data system
pub async fn init() -> Result<GlobalMarketData> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Create global market data store
    let config = store::GlobalConfig {
        num_instruments: 10_000,
        cache_size_mb: 1024,
        num_threads: num_cpus::get(),
        buffer_config: ultra_low_latency_db::core::instrument_index::InstrumentBufferConfig {
            l1_buffer_size: 1_048_576,  // 1M
            l2_buffer_size: 524_288,    // 512K
            ref_buffer_size: 65_536,    // 64K
        },
    };
    
    let market_data = GlobalMarketData::new(config)?;
    
    // Start background processing
    market_data.start_background_processing()?;
    
    Ok(market_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_tungstenite::connect_async;
    use url::Url;
    
    #[tokio::test]
    async fn test_system_integration() -> Result<()> {
        // Initialize system
        let market_data = Arc::new(init().await?);
        
        // Start WebSocket server
        let addr = "127.0.0.1:8081".parse::<SocketAddr>()?;
        let handler = WebSocketHandler::new(market_data.clone(), addr);
        
        tokio::spawn(async move {
            handler.start().await.unwrap();
        });
        
        // Connect test client
        let url = Url::parse("ws://127.0.0.1:8081")?;
        let (mut ws_stream, _) = connect_async(url).await?;
        
        // Send test messages
        for i in 0..10 {
            let msg = FeedMessage::new(
                1001 + i,  // token
                100.0,     // bid
                100.1,     // ask
                100,       // bid size
                100,       // ask size
                100.05,    // last
                50,        // last size
                i,         // seq
                feed::FeedSource::PrimaryExchange,
                feed::MessageType::L1Update,
            );
            
            ws_stream.send(serde_json::to_string(&msg)?.into()).await?;
        }
        
        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Verify data was stored
        for i in 0..10 {
            let record = market_data.get_latest_tick(1001 + i);
            assert!(record.is_some());
            let record = record.unwrap();
            assert_eq!(record.last_price, 100.05);
        }
        
        Ok(())
    }
} 