use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use deadpool_postgres::{Pool, Manager, ManagerConfig, RecyclingMethod};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc, TimeZone};
use lz4::block::compress;
use parking_lot::RwLock;
use tokio::time::sleep;
use std::time::Duration;

use crate::feed::types::FeedMessage;

pub struct TimeSeriesManager {
    #[cfg(test)]
    pub pool: Pool,
    #[cfg(not(test))]
    pool: Pool,
    config: Arc<TimeSeriesConfig>,
    stats: Arc<RwLock<TimeSeriesStats>>,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    pub partition_size_mb: usize,
    pub compression_level: CompressionLevel,
    pub cleanup_interval_sec: u64,
    pub retention_days: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionLevel {
    None,
    Low,
    Medium,
    High,
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeriesStats {
    pub records_stored: u64,
    pub bytes_written: u64,
    pub compression_ratio: f64,
    pub write_latency_ns: u64,
    pub query_latency_ns: u64,
}

impl TimeSeriesManager {
    pub fn new() -> Result<Self> {
        // Use environment variables or defaults for database configuration
        let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "ubuntu".to_string());
        let password = std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "".to_string());
        let dbname = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "market_data".to_string());
        
        let mut config = tokio_postgres::Config::new();
        config.host(&host)
            .port(port.parse().unwrap_or(5432))
            .user(&user)
            .password(&password)
            .dbname(&dbname);
            
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config.clone(), NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(16)
            .build()?;
            
        let ts_manager = Self {
            #[cfg(test)]
            pool: pool.clone(),
            #[cfg(not(test))]
            pool: pool.clone(),
            config: Arc::new(TimeSeriesConfig {
                partition_size_mb: 256,
                compression_level: CompressionLevel::High,
                cleanup_interval_sec: 3600,
                retention_days: 30,
            }),
            stats: Arc::new(RwLock::new(TimeSeriesStats::default())),
        };

        // Initialize database schema in background
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::init_database_schema(&pool_clone).await {
                tracing::error!("Failed to initialize database: {}", e);
            }
        });

        Ok(ts_manager)
    }
    
    async fn init_database_schema(pool: &Pool) -> Result<()> {
        let client = pool.get().await?;
        
        // Create the market_data table if it doesn't exist
        client.execute(
            "CREATE TABLE IF NOT EXISTS market_data (
                id BIGSERIAL PRIMARY KEY,
                token BIGINT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                bid_price DOUBLE PRECISION NOT NULL,
                ask_price DOUBLE PRECISION NOT NULL,
                bid_size INTEGER NOT NULL,
                ask_size INTEGER NOT NULL,
                last_price DOUBLE PRECISION NOT NULL,
                last_size INTEGER NOT NULL,
                sequence_num BIGINT NOT NULL,
                source VARCHAR(50) NOT NULL,
                message_type VARCHAR(50) NOT NULL,
                data BYTEA NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )",
            &[],
        ).await?;

        // Create indexes
        client.execute(
            "CREATE INDEX IF NOT EXISTS market_data_token_timestamp_idx ON market_data (token, timestamp)",
            &[],
        ).await?;

        Ok(())
    }
    
    pub async fn store_message(&self, msg: FeedMessage) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Get client from pool
        let client = self.pool.get().await?;
        
        // Serialize message for the data field
        let msg_bytes = serde_json::to_vec(&msg)?;
        let msg_bytes_len = msg_bytes.len();
        
        // Only compress if the message is large enough to benefit from compression
        let (compressed, is_compressed) = if msg_bytes_len > 1024 {
            match self.config.compression_level {
                CompressionLevel::None => (msg_bytes, false),
                _ => (compress(&msg_bytes, None, false)?, true),
            }
        } else {
            (msg_bytes, false)
        };
        
        // Store in database with all fields
        client.execute(
            "INSERT INTO market_data (
                token, timestamp, bid_price, ask_price, bid_size, ask_size,
                last_price, last_size, sequence_num, source, message_type, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            &[
                &(msg.token as i64),
                &Utc.timestamp_opt(
                    (msg.timestamp / 1_000_000_000) as i64,
                    (msg.timestamp % 1_000_000_000) as u32,
                ).unwrap(),
                &msg.bid_price,
                &msg.ask_price,
                &(msg.bid_size as i32),
                &(msg.ask_size as i32),
                &msg.last_price,
                &(msg.last_size as i32),
                &(msg.sequence_num as i64),
                &format!("{:?}", msg.source),
                &format!("{:?}", msg.message_type),
                &compressed,
            ],
        ).await?;
        
        // Update stats
        let mut stats = self.stats.write();
        stats.records_stored += 1;
        stats.bytes_written += compressed.len() as u64;
        if is_compressed {
            stats.compression_ratio = msg_bytes_len as f64 / compressed.len() as f64;
        }
        stats.write_latency_ns += start.elapsed().as_nanos() as u64;
        
        Ok(())
    }
    
    pub async fn query_range(
        &self,
        token: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<FeedMessage>> {
        let start_query = std::time::Instant::now();
        
        let client = self.pool.get().await?;
        
        let rows = client.query(
            "SELECT data FROM market_data WHERE token = $1 AND timestamp >= $2 AND timestamp <= $3",
            &[&(token as i64), &start, &end],
        ).await?;
        
        let mut messages = Vec::with_capacity(rows.len());
        
        for row in rows {
            let data: Vec<u8> = row.get(0);
            
            // Try to parse as uncompressed first
            let msg = match serde_json::from_slice(&data) {
                Ok(msg) => msg,
                Err(_) => {
                    // If that fails, try decompressing
                    let decompressed = lz4::block::decompress(&data, None)
                        .map_err(|e| anyhow!("Decompression error: {}", e))?;
                    serde_json::from_slice(&decompressed)?
                }
            };
            
            messages.push(msg);
        }
        
        // Update stats
        self.stats.write().query_latency_ns += start_query.elapsed().as_nanos() as u64;
        
        Ok(messages)
    }
    
    pub async fn create_partition(&self, date: DateTime<Utc>) -> Result<()> {
        let client = self.pool.get().await?;
        
        let partition_name = format!(
            "market_data_{}",
            date.format("%Y_%m_%d")
        );
        
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF market_data
            FOR VALUES FROM ('{}') TO ('{}')",
            partition_name,
            date.format("%Y-%m-%d 00:00:00"),
            date.format("%Y-%m-%d 23:59:59"),
        );
        
        client.execute(&query, &[]).await?;
        
        Ok(())
    }
    
    pub async fn cleanup_old_partitions(&self) -> Result<()> {
        let client = self.pool.get().await?;
        
        let cutoff_date = Utc::now() - chrono::Duration::days(self.config.retention_days as i64);
        
        let query = format!(
            "DROP TABLE IF EXISTS market_data_{}",
            cutoff_date.format("%Y_%m_%d")
        );
        
        client.execute(&query, &[]).await?;
        
        Ok(())
    }
    
    pub fn get_stats(&self) -> TimeSeriesStats {
        self.stats.read().clone()
    }

    #[cfg(test)]
    pub async fn reset_database_schema(pool: &Pool) -> Result<()> {
        let client = pool.get().await?;
        
        // Drop existing table
        client.execute("DROP TABLE IF EXISTS market_data", &[]).await?;
        
        // Recreate schema
        TimeSeriesManager::init_database_schema(pool).await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::types::{FeedSource, MessageType};
    
    #[tokio::test]
    async fn test_timeseries_connection() -> Result<()> {
        println!("Starting TimeSeriesManager connection test...");
        
        // Create TimeSeriesManager instance
        let manager = TimeSeriesManager::new()?;
        println!("TimeSeriesManager instance created successfully");
        
        // Reset database schema
        println!("Resetting database schema...");
        TimeSeriesManager::reset_database_schema(&manager.pool).await?;
        
        // Wait for schema initialization
        println!("Waiting for schema initialization...");
        sleep(Duration::from_secs(2)).await;
        
        // Get a connection from the pool to test connectivity
        let client = manager.pool.get().await?;
        println!("Successfully obtained database connection from pool");
        
        // Verify the market_data table exists
        let result = client.query_one(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'market_data'
            )",
            &[],
        ).await?;
        
        let table_exists: bool = result.get(0);
        assert!(table_exists, "market_data table should exist");
        println!("Verified market_data table exists");
        
        // Test index existence
        let index_result = client.query_one(
            "SELECT EXISTS (
                SELECT FROM pg_indexes
                WHERE tablename = 'market_data' 
                AND indexname = 'market_data_token_timestamp_idx'
            )",
            &[],
        ).await?;
        
        let index_exists: bool = index_result.get(0);
        assert!(index_exists, "market_data_token_timestamp_idx should exist");
        println!("Verified required index exists");
        
        println!("TimeSeriesManager connection test completed successfully");
        Ok(())
    }

    #[tokio::test]
    async fn test_timeseries_manager() -> Result<()> {
        let manager = TimeSeriesManager::new()?;
        
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
        
        // Store message
        manager.store_message(msg.clone()).await?;
        
        // Query back
        let start = Utc::now() - chrono::Duration::minutes(1);
        let end = Utc::now() + chrono::Duration::minutes(1);
        
        let messages = manager.query_range(1001, start, end).await?;
        
        assert!(!messages.is_empty());
        assert_eq!(messages[0].token, msg.token);
        assert_eq!(messages[0].last_price, msg.last_price);
        
        Ok(())
    }
} 