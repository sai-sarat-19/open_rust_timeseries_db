use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use deadpool_postgres::{Pool, Manager, ManagerConfig, RecyclingMethod};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc, TimeZone};
use lz4::block::compress;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::MarketDataRecord;

pub struct TimeSeriesManager {
    #[cfg(test)]
    pub pool: Pool,
    #[cfg(not(test))]
    pool: Pool,
    config: Arc<TimeSeriesConfig>,
    stats: Arc<TimeSeriesStats>,
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

#[derive(Debug, Default)]
pub struct TimeSeriesStats {
    pub records_stored: AtomicU64,
    pub bytes_written: AtomicU64,
    pub compression_ratio: AtomicU64,
    pub write_latency_ns: AtomicU64,
    pub query_latency_ns: AtomicU64,
    pub min_write_latency_ns: AtomicU64,
    pub max_write_latency_ns: AtomicU64,
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
            stats: Arc::new(TimeSeriesStats {
                min_write_latency_ns: AtomicU64::new(u64::MAX),
                ..Default::default()
            }),
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
                token BIGINT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                bid_price DOUBLE PRECISION NOT NULL,
                ask_price DOUBLE PRECISION NOT NULL,
                bid_size INTEGER NOT NULL,
                ask_size INTEGER NOT NULL,
                last_price DOUBLE PRECISION NOT NULL,
                last_size INTEGER NOT NULL,
                sequence_num BIGINT NOT NULL,
                data BYTEA NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (token, timestamp)
            )",
            &[],
        ).await?;

        // Create hypertable for time-series optimization
        client.execute(
            "SELECT create_hypertable('market_data', 'timestamp', 
             chunk_time_interval => INTERVAL '1 hour', 
             if_not_exists => TRUE)",
            &[],
        ).await?;

        // Create index for efficient querying
        client.execute(
            "CREATE INDEX IF NOT EXISTS market_data_timestamp_token_idx ON market_data (timestamp DESC, token)",
            &[],
        ).await?;

        Ok(())
    }
    
    pub async fn store_record(&self, record: &MarketDataRecord) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Get client from pool
        let client = self.pool.get().await?;
        
        // Serialize record with zero-copy where possible
        let record_bytes = unsafe {
            std::slice::from_raw_parts(
                record as *const MarketDataRecord as *const u8,
                std::mem::size_of::<MarketDataRecord>(),
            ).to_vec()
        };
        
        let record_bytes_len = record_bytes.len();
        
        // Only compress if the record is large enough
        let (compressed, is_compressed) = if record_bytes_len > 1024 {
            match self.config.compression_level {
                CompressionLevel::None => (record_bytes, false),
                _ => (compress(&record_bytes, None, false)?, true),
            }
        } else {
            (record_bytes, false)
        };
        
        // Store in database with all fields
        client.execute(
            "INSERT INTO market_data (
                token, timestamp, bid_price, ask_price, bid_size, ask_size,
                last_price, last_size, sequence_num, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &(record.token as i64),
                &Utc.timestamp_opt(
                    (record.timestamp / 1_000_000_000) as i64,
                    (record.timestamp % 1_000_000_000) as u32,
                ).unwrap(),
                &record.bid_price,
                &record.ask_price,
                &(record.bid_size as i32),
                &(record.ask_size as i32),
                &record.last_price,
                &(record.last_size as i32),
                &(record.sequence_num as i64),
                &compressed,
            ],
        ).await?;
        
        // Update stats with atomic operations
        let latency = start.elapsed().as_nanos() as u64;
        self.stats.records_stored.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(compressed.len() as u64, Ordering::Relaxed);
        if is_compressed {
            self.stats.compression_ratio.store(
                (record_bytes_len as f64 / compressed.len() as f64 * 1000.0) as u64,
                Ordering::Relaxed
            );
        }
        self.stats.write_latency_ns.fetch_add(latency, Ordering::Relaxed);
        
        // Update min/max write latency
        let mut current_min = self.stats.min_write_latency_ns.load(Ordering::Relaxed);
        while latency < current_min {
            match self.stats.min_write_latency_ns.compare_exchange_weak(
                current_min,
                latency,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        let mut current_max = self.stats.max_write_latency_ns.load(Ordering::Relaxed);
        while latency > current_max {
            match self.stats.max_write_latency_ns.compare_exchange_weak(
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
    
    pub async fn query_range(
        &self,
        token: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketDataRecord>> {
        let start_query = std::time::Instant::now();
        
        let client = self.pool.get().await?;
        
        let rows = client.query(
            "SELECT data FROM market_data WHERE token = $1 AND timestamp >= $2 AND timestamp <= $3",
            &[&(token as i64), &start, &end],
        ).await?;
        
        let mut records = Vec::with_capacity(rows.len());
        
        for row in rows {
            let data: Vec<u8> = row.get(0);
            
            // Try to parse as uncompressed first
            let record = if data.len() == std::mem::size_of::<MarketDataRecord>() {
                unsafe {
                    std::ptr::read(data.as_ptr() as *const MarketDataRecord)
                }
            } else {
                // If that fails, try decompressing
                let decompressed = lz4::block::decompress(&data, None)
                    .map_err(|e| anyhow!("Decompression error: {}", e))?;
                unsafe {
                    std::ptr::read(decompressed.as_ptr() as *const MarketDataRecord)
                }
            };
            
            records.push(record);
        }
        
        // Update query latency
        self.stats.query_latency_ns.fetch_add(
            start_query.elapsed().as_nanos() as u64,
            Ordering::Relaxed
        );
        
        Ok(records)
    }
    
    pub fn get_stats(&self) -> &TimeSeriesStats {
        &self.stats
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
    use tokio::time::sleep;
    use std::time::Duration;
    
    #[tokio::test]
    async fn test_timeseries_manager() -> Result<()> {
        let manager = TimeSeriesManager::new()?;
        
        // Reset database schema
        TimeSeriesManager::reset_database_schema(&manager.pool).await?;
        
        // Wait for schema initialization
        sleep(Duration::from_secs(2)).await;
        
        // Create test record
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
        
        // Store record
        manager.store_record(&record).await?;
        
        // Query back
        let start = Utc::now() - chrono::Duration::minutes(1);
        let end = Utc::now() + chrono::Duration::minutes(1);
        
        let records = manager.query_range(1001, start, end).await?;
        
        assert!(!records.is_empty());
        assert_eq!(records[0].token, record.token);
        assert_eq!(records[0].last_price, record.last_price);
        
        Ok(())
    }
} 