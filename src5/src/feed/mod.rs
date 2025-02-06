use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use anyhow::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1PriceUpdate {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2TradeUpdate {
    pub symbol: String,
    pub price: f64,
    pub size: f64,
    pub side: TradeSide,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
pub struct MarketDataFeed {
    pub price_updates: Arc<RwLock<Vec<L1PriceUpdate>>>,
    pub trade_updates: Arc<RwLock<Vec<L2TradeUpdate>>>,
}

impl MarketDataFeed {
    pub fn new() -> Self {
        Self {
            price_updates: Arc::new(RwLock::new(Vec::new())),
            trade_updates: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_price_update(&self, update: L1PriceUpdate) -> Result<()> {
        let mut updates = self.price_updates.write().await;
        updates.push(update);
        Ok(())
    }

    pub async fn add_trade_update(&self, update: L2TradeUpdate) -> Result<()> {
        let mut updates = self.trade_updates.write().await;
        updates.push(update);
        Ok(())
    }

    pub async fn get_latest_price(&self, symbol: &str) -> Option<L1PriceUpdate> {
        let updates = self.price_updates.read().await;
        updates.iter()
            .rev()
            .find(|update| update.symbol == symbol)
            .cloned()
    }

    pub async fn get_trades_since(&self, timestamp: i64) -> Vec<L2TradeUpdate> {
        let updates = self.trade_updates.read().await;
        updates.iter()
            .filter(|update| update.timestamp >= timestamp)
            .cloned()
            .collect()
    }
} 