use std::sync::Arc;
use redis::{Client, AsyncCommands};
use anyhow::Result;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use serde_json::json;

use crate::feed::types::FeedMessage;

pub struct RedisManager {
    client: Client,
    pub_sub: Arc<RedisPubSub>,
    stats: Arc<RwLock<RedisStats>>,
}

pub struct RedisPubSub {
    sender: broadcast::Sender<FeedMessage>,
}

#[derive(Debug, Default, Clone)]
pub struct RedisStats {
    pub messages_published: u64,
    pub subscribers: usize,
    pub publish_latency_ns: u64,
}

impl RedisManager {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let (sender, _) = broadcast::channel(10_000);
        
        Ok(Self {
            client,
            pub_sub: Arc::new(RedisPubSub { sender }),
            stats: Arc::new(RwLock::new(RedisStats::default())),
        })
    }
    
    pub async fn publish_message(&self, channel: &str, msg: &FeedMessage) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Get connection from pool
        let mut conn = self.client.get_async_connection().await?;
        
        // Convert message to JSON
        let json = json!({
            "token": msg.token,
            "bid": msg.bid_price,
            "ask": msg.ask_price,
            "last": msg.last_price,
            "timestamp": msg.timestamp,
            "source": format!("{:?}", msg.source),
            "type": format!("{:?}", msg.message_type),
        });
        
        // Publish to Redis
        let _: () = conn.publish(channel, json.to_string()).await?;
        
        // Also publish to internal broadcast channel
        // Ignore send errors as they just mean no active subscribers
        let _ = self.pub_sub.sender.send(msg.clone());
        
        // Update stats
        let mut stats = self.stats.write();
        stats.messages_published += 1;
        stats.publish_latency_ns += start.elapsed().as_nanos() as u64;
        
        Ok(())
    }
    
    pub fn subscribe(&self, _channel: &str) -> broadcast::Receiver<FeedMessage> {
        self.stats.write().subscribers += 1;
        self.pub_sub.sender.subscribe()
    }
    
    pub fn get_stats(&self) -> RedisStats {
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::types::{FeedSource, MessageType};
    use tokio::time::Duration;
    
    #[tokio::test]
    async fn test_redis_pubsub() -> Result<()> {
        let redis = RedisManager::new("redis://localhost:6379")?;
        
        // Create subscriber
        let mut rx = redis.subscribe("test_channel");
        
        // Create test message
        let msg = FeedMessage::new(
            1001,   // token
            100.0,  // bid
            100.1,  // ask
            100,    // bid size
            100,    // ask size
            100.05, // last
            50,     // last size
            1,      // seq
            FeedSource::PrimaryExchange,
            MessageType::L1Update,
        );
        
        // Publish message
        redis.publish_message("test_channel", &msg).await?;
        
        // Receive message
        tokio::select! {
            received = rx.recv() => {
                let received = received?;
                assert_eq!(received.token, msg.token);
                assert_eq!(received.last_price, msg.last_price);
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Timeout waiting for message");
            }
        }
        
        Ok(())
    }
} 