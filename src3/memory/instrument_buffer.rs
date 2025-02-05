use std::sync::Arc;
use crate::core::{
    config::{UltraLowLatencyRecord, BufferType, InstrumentBufferConfig},
    instrument_index::InstrumentIndex,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const CACHE_LINE_SIZE: usize = 64;

#[repr(align(64))]
struct RingBuffer<T: UltraLowLatencyRecord> {
    data: Vec<T>,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    capacity: usize,
}

impl<T: UltraLowLatencyRecord> RingBuffer<T> {
    fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        unsafe {
            data.set_len(capacity);
        }
        Self {
            data,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            capacity,
        }
    }

    #[inline(always)]
    unsafe fn write(&self, record: &T) -> bool {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let next_write = (write_pos + 1) % self.capacity;
        
        if next_write == self.read_pos.load(Ordering::Acquire) {
            return false;  // Buffer is full
        }

        std::ptr::write(self.data.as_ptr().add(write_pos) as *mut T, *record);
        self.write_pos.store(next_write, Ordering::Release);
        true
    }

    #[inline(always)]
    unsafe fn read(&self) -> Option<T> {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        if read_pos == self.write_pos.load(Ordering::Acquire) {
            return None;  // Buffer is empty
        }

        let record = std::ptr::read(self.data.as_ptr().add(read_pos));
        let next_read = (read_pos + 1) % self.capacity;
        self.read_pos.store(next_read, Ordering::Release);
        Some(record)
    }
}

/// Manages multiple ring buffers for a single instrument
#[repr(align(64))]
pub struct InstrumentBuffer<T: UltraLowLatencyRecord> {
    token: u64,
    l1_buffer: RingBuffer<T>,
    l2_buffer: RingBuffer<T>,
    ref_buffer: RingBuffer<T>,
    last_sequence: AtomicU64,
}

impl<T: UltraLowLatencyRecord> InstrumentBuffer<T> {
    pub fn new(token: u64, config: &InstrumentBufferConfig) -> Self {
        Self {
            token,
            l1_buffer: RingBuffer::new(config.l1_buffer_size),
            l2_buffer: RingBuffer::new(config.l2_buffer_size),
            ref_buffer: RingBuffer::new(config.ref_buffer_size),
            last_sequence: AtomicU64::new(0),
        }
    }

    /// Write a record to the specified buffer type
    #[inline(always)]
    pub unsafe fn write(&self, record: &T, buffer_type: BufferType) -> bool {
        let buffer = match buffer_type {
            BufferType::L1Price => &self.l1_buffer,
            BufferType::L2Trade => &self.l2_buffer,
            BufferType::Reference => &self.ref_buffer,
        };
        
        if record.get_sequence_num() <= self.last_sequence.load(Ordering::Relaxed) {
            return false;  // Reject out-of-sequence updates
        }
        
        if buffer.write(record) {
            self.last_sequence.store(record.get_sequence_num(), Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Read a record from the specified buffer type
    #[inline(always)]
    pub unsafe fn read(&self, buffer_type: BufferType) -> Option<T> {
        match buffer_type {
            BufferType::L1Price => self.l1_buffer.read(),
            BufferType::L2Trade => self.l2_buffer.read(),
            BufferType::Reference => self.ref_buffer.read(),
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
    pub fn register_instrument(&mut self, token: u64) -> Option<Arc<InstrumentBuffer<T>>> {
        let idx = self.index.register_instrument(token.try_into().unwrap())?;
        if self.buffers[idx].is_some() {
            return self.buffers[idx].clone();
        }

        let buffer = Arc::new(InstrumentBuffer::new(token, &self.config));
        self.buffers[idx] = Some(Arc::clone(&buffer));
        Some(buffer)
    }

    /// Write a record to a specific instrument's buffer
    #[inline]
    pub unsafe fn write(&self, token: u64, record: &T, buffer_type: BufferType) -> bool {
        if let Some(idx) = self.index.get_buffer_index(token.try_into().unwrap()) {
            if let Some(buffer) = &self.buffers[idx] {
                return buffer.write(record, buffer_type);
            }
        }
        false
    }

    /// Read a record from a specific instrument's buffer
    #[inline]
    pub unsafe fn read(&self, token: u64, buffer_type: BufferType) -> Option<T> {
        if let Some(idx) = self.index.get_buffer_index(token.try_into().unwrap()) {
            if let Some(buffer) = &self.buffers[idx] {
                return buffer.read(buffer_type);
            }
        }
        None
    }

    /// Get the index for direct access
    pub fn index(&self) -> &Arc<InstrumentIndex> {
        &self.index
    }
} 