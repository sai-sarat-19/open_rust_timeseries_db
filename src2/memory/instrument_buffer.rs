use std::sync::Arc;
use crate::core::{
    config::UltraLowLatencyRecord,
    instrument_index::{InstrumentIndex, InstrumentBufferConfig},
};
use super::zero_alloc_ring_buffer::ZeroAllocRingBuffer;

/// Types of buffers for different data categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    L1Price,    // Bid/Ask updates
    L2Trade,    // Trade updates
    Reference,  // Reference data
}

/// Manages multiple ring buffers for a single instrument
#[repr(align(64))]
pub struct InstrumentBuffer<T: UltraLowLatencyRecord> {
    // L1 price updates (bid/ask)
    l1_buffer: Arc<ZeroAllocRingBuffer<T>>,
    // L2 trade updates
    l2_buffer: Arc<ZeroAllocRingBuffer<T>>,
    // Reference data updates
    ref_buffer: Arc<ZeroAllocRingBuffer<T>>,
    // Instrument token
    token: u32,
}

impl<T: UltraLowLatencyRecord> InstrumentBuffer<T> {
    pub fn new(token: u32, config: &InstrumentBufferConfig) -> Self {
        Self {
            l1_buffer: Arc::new(ZeroAllocRingBuffer::new(config.l1_buffer_size)),
            l2_buffer: Arc::new(ZeroAllocRingBuffer::new(config.l2_buffer_size)),
            ref_buffer: Arc::new(ZeroAllocRingBuffer::new(config.ref_buffer_size)),
            token,
        }
    }

    /// Write a record to the specified buffer type
    #[inline(always)]
    pub unsafe fn write(&self, record: &T, buffer_type: BufferType) -> bool {
        match buffer_type {
            BufferType::L1Price => self.l1_buffer.write(record),
            BufferType::L2Trade => self.l2_buffer.write(record),
            BufferType::Reference => self.ref_buffer.write(record),
        }
    }

    /// Read a record from the specified buffer type
    #[inline(always)]
    pub unsafe fn read(&self, buffer_type: BufferType) -> Option<T> {
        match buffer_type {
            BufferType::L1Price => self.l1_buffer.read().map(|r| *r),
            BufferType::L2Trade => self.l2_buffer.read().map(|r| *r),
            BufferType::Reference => self.ref_buffer.read().map(|r| *r),
        }
    }

    /// Get the token for this instrument
    #[inline(always)]
    pub fn token(&self) -> u32 {
        self.token
    }

    /// Get a reference to a specific buffer
    #[inline(always)]
    pub fn get_buffer(&self, buffer_type: BufferType) -> &Arc<ZeroAllocRingBuffer<T>> {
        match buffer_type {
            BufferType::L1Price => &self.l1_buffer,
            BufferType::L2Trade => &self.l2_buffer,
            BufferType::Reference => &self.ref_buffer,
        }
    }
}

/// Manages buffers for all instruments
pub struct InstrumentBufferManager<T: UltraLowLatencyRecord> {
    // Index for fast instrument lookup
    index: Arc<InstrumentIndex>,
    // Buffers for each instrument
    buffers: Box<[Option<Arc<InstrumentBuffer<T>>>]>,
    // Buffer configuration
    config: InstrumentBufferConfig,
}

impl<T: UltraLowLatencyRecord> InstrumentBufferManager<T> {
    pub fn new(capacity: usize, config: InstrumentBufferConfig) -> Self {
        let mut buffers = Vec::with_capacity(capacity);
        buffers.resize_with(capacity, || None);

        Self {
            index: Arc::new(InstrumentIndex::new(capacity)),
            buffers: buffers.into_boxed_slice(),
            config,
        }
    }

    /// Register a new instrument and create its buffers
    pub fn register_instrument(&mut self, token: u32) -> Option<Arc<InstrumentBuffer<T>>> {
        let idx = self.index.register_instrument(token)?;
        if self.buffers[idx].is_some() {
            return self.buffers[idx].clone();
        }

        let buffer = Arc::new(InstrumentBuffer::new(token, &self.config));
        self.buffers[idx] = Some(Arc::clone(&buffer));
        Some(buffer)
    }

    /// Get the buffer for a specific instrument
    #[inline(always)]
    pub fn get_instrument_buffer(&self, token: u32) -> Option<Arc<InstrumentBuffer<T>>> {
        let idx = self.index.get_buffer_index(token)?;
        self.buffers[idx].clone()
    }

    /// Write a record to a specific instrument's buffer
    #[inline]
    pub unsafe fn write(&self, token: u32, record: &T, buffer_type: BufferType) -> bool {
        if let Some(buffer) = self.get_instrument_buffer(token) {
            buffer.write(record, buffer_type)
        } else {
            false
        }
    }

    /// Read a record from a specific instrument's buffer
    #[inline]
    pub unsafe fn read(&self, token: u32, buffer_type: BufferType) -> Option<T> {
        self.get_instrument_buffer(token)?.read(buffer_type)
    }

    /// Get the index for direct access
    pub fn index(&self) -> &Arc<InstrumentIndex> {
        &self.index
    }
} 