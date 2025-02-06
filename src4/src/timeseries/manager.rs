use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use deadpool_postgres::{Pool, Manager, ManagerConfig, RecyclingMethod};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc, TimeZone};
use lz4::block::compress;
use parking_lot::RwLock;

use crate::feed::types::FeedMessage;

pub struct TimeSeriesManager {
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
        let mut config = tokio_postgres::Config::new();
        config.host("localhost")
            .port(5432)
            .user("postgres")
            .password("postgres")
            .dbname("market_data");
            
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config.clone(), NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(16)
            .build()?;
            
        Ok(Self {
            pool,
            config: Arc::new(TimeSeriesConfig {
                partition_size_mb: 256,
                compression_level: CompressionLevel::High,
                cleanup_interval_sec: 3600,
                retention_days: 30,
            }),
            stats: Arc::new(RwLock::new(TimeSeriesStats::default())),
        })
    }
    
    pub async fn store_message(&self, msg: FeedMessage) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Get client from pool
        let client = self.pool.get().await?;
        
        // Compress message
        let msg_bytes = serde_json::to_vec(&msg)?;
        let msg_bytes_len = msg_bytes.len();
        let compressed = match self.config.compression_level {
            CompressionLevel::None => msg_bytes,
            _ => compress(&msg_bytes, None, false)?,
        };
        
        // Store in database
        client.execute(
            "INSERT INTO market_data (token, timestamp, data) VALUES ($1, $2, $3)",
            &[
                &(msg.token as i64),
                &Utc.timestamp_opt(
                    (msg.timestamp / 1_000_000_000) as i64,
                    (msg.timestamp % 1_000_000_000) as u32,
                ).unwrap(),
                &compressed,
            ],
        ).await?;
        
        // Update stats
        let mut stats = self.stats.write();
        stats.records_stored += 1;
        stats.bytes_written += compressed.len() as u64;
        stats.compression_ratio = msg_bytes_len as f64 / compressed.len() as f64;
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
            let compressed: Vec<u8> = row.get(0);
            let decompressed = match self.config.compression_level {
                CompressionLevel::None => compressed,
                _ => lz4::block::decompress(&compressed, None)?,
            };
            
            let msg: FeedMessage = serde_json::from_slice(&decompressed)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::types::{FeedSource, MessageType};
    
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