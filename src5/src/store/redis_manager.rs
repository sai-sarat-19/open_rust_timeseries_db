use std::sync::Arc;
use redis::{Client, AsyncCommands};
use anyhow::Result;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::MarketDataRecord;

#[derive(Debug)]
pub struct RedisManager {
    client: Client,
    pub_sub: Arc<RedisPubSub>,
    stats: Arc<RedisStats>,
}

#[derive(Debug)]
pub struct RedisPubSub {
    sender: broadcast::Sender<MarketDataRecord>,
}

#[derive(Debug, Default)]
pub struct RedisStats {
    pub messages_published: AtomicU64,
    pub subscribers: AtomicU64,
    pub publish_latency_ns: AtomicU64,
    pub min_latency_ns: AtomicU64,
    pub max_latency_ns: AtomicU64,
}

impl RedisManager {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let (sender, _) = broadcast::channel(10_000);
        
        Ok(Self {
            client,
            pub_sub: Arc::new(RedisPubSub { sender }),
            stats: Arc::new(RedisStats {
                min_latency_ns: AtomicU64::new(u64::MAX),
                ..Default::default()
            }),
        })
    }
    
    pub async fn publish_message(&self, channel: &str, record: &MarketDataRecord) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Get connection from pool
        let mut conn = self.client.get_async_connection().await?;
        
        // Convert to JSON with minimal allocations
        let json = json!({
            "token": record.token,
            "bid": record.bid_price,
            "ask": record.ask_price,
            "bid_size": record.bid_size,
            "ask_size": record.ask_size,
            "last": record.last_price,
            "last_size": record.last_size,
            "seq": record.sequence_num,
            "ts": record.timestamp,
        });
        
        // Publish to Redis
        let _: () = conn.publish(channel, json.to_string()).await?;
        
        // Also publish to internal broadcast channel
        let _ = self.pub_sub.sender.send(*record);
        
        // Update stats with atomic operations
        let latency = start.elapsed().as_nanos() as u64;
        self.stats.messages_published.fetch_add(1, Ordering::Relaxed);
        self.stats.publish_latency_ns.fetch_add(latency, Ordering::Relaxed);
        
        // Update min/max latency
        let mut current_min = self.stats.min_latency_ns.load(Ordering::Relaxed);
        while latency < current_min {
            match self.stats.min_latency_ns.compare_exchange_weak(
                current_min,
                latency,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        let mut current_max = self.stats.max_latency_ns.load(Ordering::Relaxed);
        while latency > current_max {
            match self.stats.max_latency_ns.compare_exchange_weak(
                current_max,
                latency,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
        
        Ok(())
    }
    
    pub fn subscribe(&self, _channel: &str) -> broadcast::Receiver<MarketDataRecord> {
        self.stats.subscribers.fetch_add(1, Ordering::Relaxed);
        self.pub_sub.sender.subscribe()
    }
    
    pub fn get_stats(&self) -> &RedisStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;
    
    #[tokio::test]
    async fn test_redis_pubsub() -> Result<()> {
        let redis = RedisManager::new("redis://localhost:6379")?;
        
        // Create subscriber
        let mut rx = redis.subscribe("test_channel");
        
        // Create test message
        let record = MarketDataRecord::new(
            1001,   // token
            100.0,  // bid
            100.1,  // ask
            100,    // bid size
            100,    // ask size
            100.05, // last
            50,     // last size
            1,      // seq
            unsafe { crate::rdtsc_timestamp() },
            0,      // flags
        );
        
        // Publish message
        redis.publish_message("test_channel", &record).await?;
        
        // Receive message
        tokio::select! {
            received = rx.recv() => {
                let received = received?;
                assert_eq!(received.token, record.token);
                assert_eq!(received.last_price, record.last_price);
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Timeout waiting for message");
            }
        }
        
        Ok(())
    }
} 