use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use futures::{StreamExt, SinkExt};
use anyhow::Result;
use parking_lot::RwLock;

use crate::store::GlobalMarketData;
use crate::feed::types::{FeedMessage, FeedStats, FeedSource, MessageType};
use crate::{GlobalConfig, InstrumentBufferConfig};

#[derive(Clone)]
pub struct WebSocketHandler {
    market_data: Arc<GlobalMarketData>,
    stats: Arc<RwLock<FeedStats>>,
    address: SocketAddr,
}

impl WebSocketHandler {
    pub fn new(market_data: Arc<GlobalMarketData>, address: SocketAddr) -> Self {
        Self {
            market_data,
            stats: Arc::new(RwLock::new(FeedStats::default())),
            address,
        }
    }
    
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(self.address).await?;
        tracing::info!("WebSocket server listening on {}", self.address);
        
        while let Ok((stream, addr)) = listener.accept().await {
            tracing::info!("New connection from {}", addr);
            
            let market_data = Arc::clone(&self.market_data);
            let stats = Arc::clone(&self.stats);
            
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, market_data, stats).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
        
        Ok(())
    }
    
    pub fn get_stats(&self) -> FeedStats {
        *self.stats.read()
    }
}

async fn handle_connection(
    stream: TcpStream,
    market_data: Arc<GlobalMarketData>,
    stats: Arc<RwLock<FeedStats>>,
) -> Result<()> {
    let ws_stream = accept_async(stream).await?;
    let (mut write, mut read) = ws_stream.split();
    
    // Send initial heartbeat
    write.send(serde_json::to_string(&create_heartbeat())?.into()).await?;
    
    // Create a channel for message processing with backpressure
    let (tx, mut rx) = tokio::sync::mpsc::channel::<FeedMessage>(1000);
    
    // Spawn message processing task
    let market_data_clone = Arc::clone(&market_data);
    let stats_clone = Arc::clone(&stats);
    let process_task = tokio::spawn(async move {
        while let Some(feed_msg) = rx.recv().await {
            let start = std::time::Instant::now();
            
            if let Err(e) = market_data_clone.process_feed_message(feed_msg.clone()).await {
                tracing::error!("Error processing message: {}", e);
                stats_clone.write().invalid_messages += 1;
            } else {
                // Publish to Redis directly
                if let Some(redis) = market_data_clone.get_redis() {
                    if let Err(e) = redis.publish_message("market_data", &feed_msg).await {
                        tracing::error!("Error publishing to Redis: {}", e);
                    }
                }
                
                let mut stats = stats_clone.write();
                stats.messages_processed += 1;
                stats.processing_time_ns += start.elapsed().as_nanos() as u64;
            }
        }
    });
    
    while let Some(msg) = read.next().await {
        let msg = msg?;
        
        // Update received count
        stats.write().messages_received += 1;
        
        // Process message
        if msg.is_text() {
            match serde_json::from_str::<FeedMessage>(msg.to_text()?) {
                Ok(feed_msg) => {
                    if feed_msg.is_valid() {
                        // Send to processing channel with backpressure
                        if let Err(e) = tx.send(feed_msg).await {
                            tracing::error!("Error sending to processing channel: {}", e);
                            stats.write().invalid_messages += 1;
                        }
                    } else {
                        stats.write().invalid_messages += 1;
                    }
                }
                Err(e) => {
                    tracing::error!("Error parsing message: {}", e);
                    stats.write().invalid_messages += 1;
                }
            }
        }
    }
    
    // Wait for processing to complete
    drop(tx);
    if let Err(e) = process_task.await {
        tracing::error!("Error in message processing task: {}", e);
    }
    
    Ok(())
}

fn create_heartbeat() -> FeedMessage {
    FeedMessage::new(
        0,              // token
        0.0,           // bid_price
        0.0,           // ask_price
        0,             // bid_size
        0,             // ask_size
        0.0,           // last_price
        0,             // last_size
        0,             // sequence_num
        FeedSource::PrimaryExchange,
        MessageType::HeartBeat,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::connect_async;
    use url::Url;
    
    #[tokio::test]
    async fn test_websocket_handler() -> Result<()> {
        // Setup market data store
        let config = GlobalConfig {
            num_instruments: 1000,
            cache_size_mb: 1024,
            num_threads: 4,
            buffer_config: InstrumentBufferConfig {
                l1_buffer_size: 65536,
                l2_buffer_size: 32768,
                ref_buffer_size: 8192,
            },
        };
        
        let market_data = Arc::new(GlobalMarketData::new(config)?);
        
        // Start WebSocket server
        let addr = "127.0.0.1:8080".parse::<SocketAddr>()?;
        let handler = WebSocketHandler::new(market_data.clone(), addr);
        
        tokio::spawn(async move {
            handler.start().await.unwrap();
        });
        
        // Connect test client
        let url = Url::parse("ws://127.0.0.1:8080")?;
        let (mut ws_stream, _) = connect_async(url).await?;
        
        // Send test message
        let test_msg = FeedMessage::new(
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
        
        ws_stream.send(serde_json::to_string(&test_msg)?.into()).await?;
        
        // Verify message was processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        if let Some(record) = market_data.get_latest_tick(1001) {
            assert_eq!(record.last_price, 100.05);
            assert_eq!(record.bid_price, 100.0);
            assert_eq!(record.ask_price, 100.1);
        } else {
            panic!("No market data record found");
        }
        
        Ok(())
    }
} 