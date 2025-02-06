use std::sync::Arc;
use crate::core::{
    config::UltraLowLatencyRecord,
    instrument_index::InstrumentBufferConfig,
};
use crate::memory::{
    instrument_buffer::{InstrumentBufferManager, BufferType},
};

/// A thread-safe, ultra-low-latency database with instrument-specific buffers
#[derive(Clone)]
pub struct UltraLowLatencyDB<T: UltraLowLatencyRecord> {
    buffer_manager: Arc<InstrumentBufferManager<T>>,
}

impl<T: UltraLowLatencyRecord> UltraLowLatencyDB<T> {
    pub fn new(capacity: usize) -> Self {
        let config = InstrumentBufferConfig::default();
        Self::with_config(capacity, config)
    }

    pub fn with_config(capacity: usize, config: InstrumentBufferConfig) -> Self {
        Self {
            buffer_manager: Arc::new(InstrumentBufferManager::new(capacity, config)),
        }
    }

    /// Write a record to the L1 price buffer for the specified instrument
    #[inline(always)]
    pub fn write(&self, record: &T) -> bool {
        unsafe {
            self.buffer_manager.write(record.symbol_id(), record, BufferType::L1Price)
        }
    }

    /// Read a record from the L1 price buffer for the specified instrument
    #[inline(always)]
    pub fn read(&self) -> Option<T> {
        unsafe {
            // For now, just read from the first registered instrument
            // In practice, you'd want to specify which instrument to read from
            let token = self.buffer_manager.index().get_first_token()?;
            self.buffer_manager.read(token, BufferType::L1Price)
        }
    }

    /// Write a record to a specific buffer type for an instrument
    #[inline]
    pub fn write_to_buffer(&self, token: u32, record: &T, buffer_type: BufferType) -> bool {
        unsafe {
            self.buffer_manager.write(token, record, buffer_type)
        }
    }

    /// Read a record from a specific buffer type for an instrument
    #[inline]
    pub fn read_from_buffer(&self, token: u32, buffer_type: BufferType) -> Option<T> {
        unsafe {
            self.buffer_manager.read(token, buffer_type)
        }
    }

    /// Get the buffer manager for direct access
    pub fn buffer_manager(&self) -> &Arc<InstrumentBufferManager<T>> {
        &self.buffer_manager
    }
} 