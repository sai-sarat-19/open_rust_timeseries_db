use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedMessage {
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
    pub source: FeedSource,
    pub message_type: MessageType,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FeedSource {
    PrimaryExchange,
    SecondaryVenue,
    DarkPool,
    Reference,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    L1Update,
    L2Update,
    Trade,
    ReferenceData,
    HeartBeat,
}

impl FeedMessage {
    pub fn new(
        token: u64,
        bid_price: f64,
        ask_price: f64,
        bid_size: u32,
        ask_size: u32,
        last_price: f64,
        last_size: u32,
        sequence_num: u64,
        source: FeedSource,
        message_type: MessageType,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
            
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
            flags: 0,
            source,
            message_type,
        }
    }
    
    pub fn is_valid(&self) -> bool {
        self.bid_price > 0.0 
            && self.ask_price > 0.0 
            && self.ask_price >= self.bid_price
            && self.bid_size > 0 
            && self.ask_size > 0
    }
    
    pub fn set_flag(&mut self, flag: u8) {
        self.flags |= flag;
    }
    
    pub fn clear_flag(&mut self, flag: u8) {
        self.flags &= !flag;
    }
    
    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FeedStats {
    pub messages_received: u64,
    pub messages_processed: u64,
    pub invalid_messages: u64,
    pub processing_time_ns: u64,
}

impl Default for FeedStats {
    fn default() -> Self {
        Self {
            messages_received: 0,
            messages_processed: 0,
            invalid_messages: 0,
            processing_time_ns: 0,
        }
    }
}

// Message flags
pub const FLAG_SNAPSHOT: u8 = 0x01;
pub const FLAG_RECOVERY: u8 = 0x02;
pub const FLAG_DUPLICATE: u8 = 0x04;
pub const FLAG_STALE: u8 = 0x08;
pub const FLAG_CORRECTED: u8 = 0x10;
pub const FLAG_CLOSING: u8 = 0x20;
pub const FLAG_OPENING: u8 = 0x40;
pub const FLAG_AUCTION: u8 = 0x80; 