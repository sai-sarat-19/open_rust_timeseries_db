use std::sync::Arc;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use anyhow::Result;
use url::Url;
use tokio_tungstenite::connect_async;
use chrono::Utc;
use futures::SinkExt;

use crate::{
    FeedMessage, FeedSource, MessageType, WebSocketHandler,
    GlobalMarketData, GlobalConfig,
    TimeSeriesManager, TimeSeriesConfig, CompressionLevel,
    RedisManager, InstrumentBufferConfig,
};

#[tokio::test]
pub async fn test_full_system_integration() -> Result<()> {
    println!("Starting test_full_system_integration...");
    
    // Initialize components
    println!("Setting up system components...");
    let redis = Arc::new(RedisManager::new("redis://localhost:6379")?);
    let market_data = Arc::new(GlobalMarketData::new_with_redis(GlobalConfig {
        num_instruments: 10_000,
        cache_size_mb: 1024,
        num_threads: num_cpus::get(),
        buffer_config: InstrumentBufferConfig {
            l1_buffer_size: 1_048_576,  // 1M
            l2_buffer_size: 524_288,    // 512K
            ref_buffer_size: 65_536,    // 64K
        },
    }, Arc::clone(&redis))?);
    let time_series = TimeSeriesManager::new()?;
    println!("System components initialized successfully");
    
    // Start WebSocket server
    println!("Starting WebSocket server...");
    let ws_handler = start_websocket_server(market_data.clone()).await?;
    println!("WebSocket server started successfully");
    
    // Create test client
    println!("Connecting test client...");
    let mut ws_client = connect_test_client().await?;
    println!("Test client connected successfully");
    
    // Create Redis subscriber
    println!("Creating Redis subscriber...");
    let mut redis_rx = redis.subscribe("market_data");
    println!("Redis subscriber created successfully");
    
    // Test different message types and flows
    println!("Starting L1 price updates test...");
    test_l1_price_updates(&mut ws_client, &market_data, &mut redis_rx).await?;
    
    println!("Testing L2 trade updates...");
    test_l2_trade_updates(&mut ws_client, &market_data, &mut redis_rx).await?;
    
    println!("Testing historical data...");
    test_historical_data(&time_series).await?;
    
    println!("Testing high throughput...");
    test_high_throughput(&mut ws_client, &market_data).await?;
    
    // Print statistics
    print_system_stats(&market_data, &redis, &time_series);
    
    Ok(())
}

async fn setup_system() -> Result<(Arc<GlobalMarketData>, RedisManager, TimeSeriesManager)> {
    // Configure the system
    let config = GlobalConfig {
        num_instruments: 10_000,
        cache_size_mb: 1024,
        num_threads: num_cpus::get(),
        buffer_config: InstrumentBufferConfig {
            l1_buffer_size: 1_048_576,  // 1M
            l2_buffer_size: 524_288,    // 512K
            ref_buffer_size: 65_536,    // 64K
        },
    };
    
    // Initialize components
    let market_data = Arc::new(GlobalMarketData::new(config)?);
    let redis = RedisManager::new("redis://localhost:6379")?;
    let time_series = TimeSeriesManager::new()?;
    
    // Start background processing
    market_data.start_background_processing()?;
    
    Ok((market_data, redis, time_series))
}

async fn start_websocket_server(market_data: Arc<GlobalMarketData>) -> Result<WebSocketHandler> {
    let addr = "127.0.0.1:8082".parse::<SocketAddr>()?;
    let handler = WebSocketHandler::new(market_data, addr);
    let handler_clone = handler.clone();
    
    tokio::spawn(async move {
        handler_clone.start().await.unwrap();
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    Ok(handler)
}

async fn connect_test_client() -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let url = Url::parse("ws://127.0.0.1:8082")?;
    let (ws_stream, _) = connect_async(url).await?;
    Ok(ws_stream)
}

async fn test_l1_price_updates(
    ws_client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    market_data: &GlobalMarketData,
    redis_rx: &mut tokio::sync::broadcast::Receiver<FeedMessage>,
) -> Result<()> {
    println!("Creating L1 update message...");
    // Send L1 update
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
    
    println!("Sending message via WebSocket...");
    let json_msg = serde_json::to_string(&msg)?;
    println!("Message JSON: {}", json_msg);
    ws_client.send(json_msg.into()).await?;
    println!("Message sent successfully");
    
    // Verify market data update
    println!("Waiting for market data update...");
    sleep(Duration::from_millis(100)).await;
    match market_data.get_latest_tick(1001) {
        Some(record) => {
            println!("Market data record found: {:?}", record);
            assert_eq!(record.last_price, 100.05);
        }
        None => {
            println!("No market data record found!");
            return Err(anyhow::anyhow!("No market data record found"));
        }
    }
    
    // Verify Redis publication
    println!("Waiting for Redis message...");
    let mut retry_count = 0;
    let max_retries = 5;
    
    while retry_count < max_retries {
        match tokio::time::timeout(Duration::from_secs(1), redis_rx.recv()).await {
            Ok(Ok(redis_msg)) => {
                println!("Redis message received: {:?}", redis_msg);
                if redis_msg.token == msg.token {
                    assert_eq!(redis_msg.token, msg.token);
                    println!("L1 price updates test completed successfully");
                    return Ok(());
                } else {
                    println!("Received message for different token: {}", redis_msg.token);
                }
            }
            Ok(Err(e)) => {
                println!("Redis receive error (attempt {}): {}", retry_count + 1, e);
            }
            Err(_) => {
                println!("Redis receive timeout (attempt {})", retry_count + 1);
            }
        }
        retry_count += 1;
        sleep(Duration::from_millis(100)).await;
    }
    
    println!("Redis receive timeout after {} retries!", max_retries);
    Err(anyhow::anyhow!("Redis receive timeout after {} retries", max_retries))
}

async fn test_l2_trade_updates(
    ws_client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    market_data: &GlobalMarketData,
    redis_rx: &mut tokio::sync::broadcast::Receiver<FeedMessage>,
) -> Result<()> {
    // Send L2 trade
    let msg = FeedMessage::new(
        1002,   // token
        101.0,  // bid
        101.1,  // ask
        200,    // bid size
        200,    // ask size
        101.05, // last
        150,    // last size
        1,      // seq
        FeedSource::SecondaryVenue,
        MessageType::L2Update,
    );
    
    ws_client.send(serde_json::to_string(&msg)?.into()).await?;
    
    // Verify market data update
    sleep(Duration::from_millis(10)).await;
    let record = market_data.get_latest_tick(1002).expect("No market data record found");
    assert_eq!(record.last_price, 101.05);
    
    // Verify Redis publication
    let redis_msg = redis_rx.recv().await?;
    assert_eq!(redis_msg.token, msg.token);
    
    Ok(())
}

async fn test_historical_data(time_series: &TimeSeriesManager) -> Result<()> {
    // Create test message
    let msg = FeedMessage::new(
        1003,   // token
        102.0,  // bid
        102.1,  // ask
        300,    // bid size
        300,    // ask size
        102.05, // last
        250,    // last size
        1,      // seq
        FeedSource::PrimaryExchange,
        MessageType::L1Update,
    );
    
    // Store message
    time_series.store_message(msg.clone()).await?;
    
    // Query back
    let start = Utc::now() - chrono::Duration::minutes(1);
    let end = Utc::now() + chrono::Duration::minutes(1);
    
    let messages = time_series.query_range(1003, start, end).await?;
    assert!(!messages.is_empty());
    assert_eq!(messages[0].token, msg.token);
    
    Ok(())
}

async fn test_high_throughput(
    ws_client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    market_data: &GlobalMarketData,
) -> Result<()> {
    const NUM_MESSAGES: u64 = 10_000;
    let start = std::time::Instant::now();
    
    for i in 0..NUM_MESSAGES {
        let msg = FeedMessage::new(
            2001 + i,  // token
            100.0,     // bid
            100.1,     // ask
            100,       // bid size
            100,       // ask size
            100.05,    // last
            50,        // last size
            i,         // seq
            FeedSource::PrimaryExchange,
            MessageType::L1Update,
        );
        
        ws_client.send(serde_json::to_string(&msg)?.into()).await?;
    }
    
    // Wait for processing
    sleep(Duration::from_millis(100)).await;
    
    let elapsed = start.elapsed();
    let rate = NUM_MESSAGES as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.2} messages/second", rate);
    
    // Verify all messages were processed
    let stats = market_data.get_stats();
    assert!(stats.total_messages >= NUM_MESSAGES);
    
    Ok(())
}

fn print_system_stats(
    market_data: &GlobalMarketData,
    redis: &RedisManager,
    time_series: &TimeSeriesManager,
) {
    let md_stats = market_data.get_stats();
    let redis_stats = redis.get_stats();
    let ts_stats = time_series.get_stats();
    
    println!("\nSystem Statistics:");
    println!("=================");
    println!("Market Data:");
    println!("  Total Messages: {}", md_stats.total_messages);
    println!("  Total Updates: {}", md_stats.total_updates);
    println!("  Buffer Full Count: {}", md_stats.buffer_full_count);
    println!("  Subscriber Count: {}", md_stats.subscriber_count);
    
    println!("\nRedis:");
    println!("  Messages Published: {}", redis_stats.messages_published);
    println!("  Subscribers: {}", redis_stats.subscribers);
    println!("  Avg Publish Latency: {} ns", 
        redis_stats.publish_latency_ns / redis_stats.messages_published.max(1));
    
    println!("\nTime Series:");
    println!("  Records Stored: {}", ts_stats.records_stored);
    println!("  Bytes Written: {}", ts_stats.bytes_written);
    println!("  Compression Ratio: {:.2}", ts_stats.compression_ratio);
    println!("  Avg Write Latency: {} ns",
        ts_stats.write_latency_ns / ts_stats.records_stored.max(1));
    println!("  Avg Query Latency: {} ns",
        ts_stats.query_latency_ns / ts_stats.records_stored.max(1));
} 