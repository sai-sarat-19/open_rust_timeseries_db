use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::RwLock;
use crossbeam::queue::SegQueue;
use anyhow::Result;
use thiserror::Error;

use crate::feed::types::FeedMessage;
use crate::timeseries::TimeSeriesManager;

// Import src3's components
use ultra_low_latency_db::core::{
    market_data::MarketDataRecord,
    instrument_index::InstrumentBufferConfig,
    config::BufferType,
};
use ultra_low_latency_db::memory::instrument_buffer::InstrumentBufferManager;

#[derive(Error, Debug)]
pub enum MarketDataError {
    #[error("Buffer full")]
    BufferFull,
    #[error("Invalid instrument: {0}")]
    InvalidInstrument(u32),
    #[error("Feed error: {0}")]
    FeedError(String),
}

/// Callback type for market data subscriptions
type MarketDataCallback = Box<dyn Fn(&MarketDataRecord) + Send + Sync>;

/// Global Market Data Store that integrates src3's buffer system with the feed module
pub struct GlobalMarketData {
    // Core buffer management from src3
    buffer_manager: Arc<InstrumentBufferManager<MarketDataRecord>>,
    
    // Subscriber management
    subscribers: Arc<DashMap<u32, Vec<MarketDataCallback>>>,
    
    // Time series management for historical data
    time_series: Arc<TimeSeriesManager>,
    
    // Configuration
    config: Arc<GlobalConfig>,
    
    // Statistics and monitoring
    stats: Arc<RwLock<MarketDataStats>>,
    
    // Message queue for background processing
    background_queue: Arc<SegQueue<FeedMessage>>,
}

#[derive(Debug)]
pub struct GlobalConfig {
    pub num_instruments: usize,
    pub cache_size_mb: usize,
    pub num_threads: usize,
    pub buffer_config: InstrumentBufferConfig,
}

#[derive(Debug, Default)]
pub struct MarketDataStats {
    pub total_messages: u64,
    pub total_updates: u64,
    pub buffer_full_count: u64,
    pub invalid_messages: u64,
    pub subscriber_count: usize,
}

impl GlobalMarketData {
    pub fn new(config: GlobalConfig) -> Result<Self> {
        let buffer_manager = Arc::new(InstrumentBufferManager::new(
            config.num_instruments,
            config.buffer_config.clone(),
        ));
        
        Ok(Self {
            buffer_manager,
            subscribers: Arc::new(DashMap::new()),
            time_series: Arc::new(TimeSeriesManager::new()?),
            config: Arc::new(config),
            stats: Arc::new(RwLock::new(MarketDataStats::default())),
            background_queue: Arc::new(SegQueue::new()),
        })
    }
    
    /// Process a new feed message
    pub fn process_feed_message(&self, msg: FeedMessage) -> Result<(), MarketDataError> {
        // Convert feed message to market data record
        let record = self.convert_feed_message(msg)?;
        
        // Update the buffer
        unsafe {
            if !self.buffer_manager.write(
                record.get_token(),
                &record,
                BufferType::L1Price,
            ) {
                self.stats.write().buffer_full_count += 1;
                return Err(MarketDataError::BufferFull);
            }
        }
        
        // Notify subscribers
        if let Some(subscribers) = self.subscribers.get(&record.symbol_id()) {
            for callback in subscribers.iter() {
                callback(&record);
            }
        }
        
        // Queue for time series storage
        self.background_queue.push(msg);
        
        // Update stats
        let mut stats = self.stats.write();
        stats.total_messages += 1;
        stats.total_updates += 1;
        
        Ok(())
    }
    
    /// Get the latest tick for an instrument
    pub fn get_latest_tick(&self, token: u32) -> Option<MarketDataRecord> {
        unsafe {
            self.buffer_manager.read(token as u64, BufferType::L1Price)
        }
    }
    
    /// Subscribe to updates for an instrument
    pub fn subscribe(&self, token: u32, callback: MarketDataCallback) {
        self.subscribers
            .entry(token)
            .or_default()
            .push(callback);
            
        self.stats.write().subscriber_count += 1;
    }
    
    /// Start background processing
    pub fn start_background_processing(&self) -> Result<()> {
        let queue = Arc::clone(&self.background_queue);
        let time_series = Arc::clone(&self.time_series);
        
        tokio::spawn(async move {
            while let Some(msg) = queue.pop() {
                if let Err(e) = time_series.store_message(msg).await {
                    tracing::error!("Error storing message: {}", e);
                }
            }
        });
        
        Ok(())
    }
    
    /// Convert a feed message to a market data record
    fn convert_feed_message(&self, msg: FeedMessage) -> Result<MarketDataRecord, MarketDataError> {
        Ok(MarketDataRecord::new(
            msg.token,
            msg.bid_price,
            msg.ask_price,
            msg.bid_size,
            msg.ask_size,
            msg.last_price,
            msg.last_size,
            msg.timestamp,
            msg.sequence_num,
            msg.flags,
        ))
    }
    
    /// Get current statistics
    pub fn get_stats(&self) -> MarketDataStats {
        self.stats.read().clone()
    }
}

// Implement Send + Sync for GlobalMarketData
unsafe impl Send for GlobalMarketData {}
unsafe impl Sync for GlobalMarketData {} 