use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::RwLock;
use crossbeam::queue::SegQueue;
use anyhow::Result;
use thiserror::Error;

use crate::feed::types::FeedMessage;
use crate::timeseries::TimeSeriesManager;
use crate::store::redis_manager::RedisManager;
use crate::InstrumentBufferConfig;

#[derive(Error, Debug)]
pub enum MarketDataError {
    #[error("Buffer full")]
    BufferFull,
    #[error("Invalid instrument: {0}")]
    InvalidInstrument(u32),
    #[error("Feed error: {0}")]
    FeedError(String),
}

/// Market data record type
#[derive(Debug, Clone, Copy)]
pub struct MarketDataRecord {
    pub token: u64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: u32,
    pub ask_size: u32,
    pub last_price: f64,
    pub last_size: u32,
    pub timestamp: u64,
    pub sequence_num: u64,
    pub flags: u8,
}

impl MarketDataRecord {
    pub fn new(
        token: u64,
        bid_price: f64,
        ask_price: f64,
        bid_size: u32,
        ask_size: u32,
        last_price: f64,
        last_size: u32,
        timestamp: u64,
        sequence_num: u64,
        flags: u8,
    ) -> Self {
        Self {
            token,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            last_price,
            last_size,
            timestamp,
            sequence_num,
            flags,
        }
    }
    
    pub fn get_token(&self) -> u64 {
        self.token
    }
    
    pub fn symbol_id(&self) -> u32 {
        self.token as u32
    }
}

/// Callback type for market data subscriptions
type MarketDataCallback = Box<dyn Fn(&MarketDataRecord) + Send + Sync>;

/// Global Market Data Store
pub struct GlobalMarketData {
    // Buffer management
    buffer_manager: Arc<DashMap<u64, MarketDataRecord>>,
    
    // Subscriber management
    subscribers: Arc<DashMap<u32, Vec<MarketDataCallback>>>,
    
    // Time series management for historical data
    time_series: Arc<TimeSeriesManager>,
    
    // Redis manager for real-time distribution
    redis: Arc<RedisManager>,
    
    // Configuration
    config: Arc<GlobalConfig>,
    
    // Statistics and monitoring
    stats: Arc<RwLock<MarketDataStats>>,
    
    // Message queue for background processing
    background_queue: Arc<SegQueue<FeedMessage>>,
}

#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub num_instruments: usize,
    pub cache_size_mb: usize,
    pub num_threads: usize,
    pub buffer_config: InstrumentBufferConfig,
}

#[derive(Debug, Default, Clone)]
pub struct MarketDataStats {
    pub total_messages: u64,
    pub total_updates: u64,
    pub buffer_full_count: u64,
    pub invalid_messages: u64,
    pub subscriber_count: usize,
}

impl GlobalMarketData {
    pub fn new(config: GlobalConfig) -> Result<Self> {
        let redis = Arc::new(RedisManager::new("redis://localhost:6379")?);
        
        Ok(Self {
            buffer_manager: Arc::new(DashMap::new()),
            subscribers: Arc::new(DashMap::new()),
            time_series: Arc::new(TimeSeriesManager::new()?),
            redis,
            config: Arc::new(config),
            stats: Arc::new(RwLock::new(MarketDataStats::default())),
            background_queue: Arc::new(SegQueue::new()),
        })
    }
    
    pub fn new_with_redis(config: GlobalConfig, redis: Arc<RedisManager>) -> Result<Self> {
        Ok(Self {
            buffer_manager: Arc::new(DashMap::new()),
            subscribers: Arc::new(DashMap::new()),
            time_series: Arc::new(TimeSeriesManager::new()?),
            redis,
            config: Arc::new(config),
            stats: Arc::new(RwLock::new(MarketDataStats::default())),
            background_queue: Arc::new(SegQueue::new()),
        })
    }
    
    /// Process a new feed message
    pub async fn process_feed_message(&self, msg: FeedMessage) -> Result<(), MarketDataError> {
        // Convert feed message to market data record
        let record = self.convert_feed_message(&msg)?;
        
        // Update the buffer
        self.buffer_manager.insert(record.get_token(), record);
        
        // Notify subscribers
        if let Some(subscribers) = self.subscribers.get(&record.symbol_id()) {
            for callback in subscribers.iter() {
                callback(&record);
            }
        }
        
        // Publish to Redis
        if let Err(e) = self.redis.publish_message("market_data", &msg).await {
            tracing::error!("Failed to publish to Redis: {}", e);
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
        self.buffer_manager.get(&(token as u64)).map(|r| *r)
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
        let queue = Arc::clone(&self.background_queue) as Arc<SegQueue<FeedMessage>>;
        let time_series = Arc::clone(&self.time_series) as Arc<TimeSeriesManager>;
        
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
    fn convert_feed_message(&self, msg: &FeedMessage) -> Result<MarketDataRecord, MarketDataError> {
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
    
    /// Get reference to Redis manager
    pub fn get_redis(&self) -> Option<Arc<RedisManager>> {
        Some(Arc::clone(&self.redis))
    }
    
    /// Get current statistics
    pub fn get_stats(&self) -> MarketDataStats {
        self.stats.read().clone()
    }
}

// Implement Send + Sync for GlobalMarketData
unsafe impl Send for GlobalMarketData {}
unsafe impl Sync for GlobalMarketData {} 