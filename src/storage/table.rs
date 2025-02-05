use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use dashmap::DashMap;

use crate::memory::low_latency_mpmc_ring::LowLatencyMpmcRing;

// Cache line size for alignment
const CACHE_LINE_SIZE: usize = 64;

#[derive(Clone)]
#[repr(align(64))]  // Align to cache line
pub struct FieldConfig {
    pub field_size_bytes: usize,
    pub ring_capacity: usize,
}

#[derive(Clone)]
pub struct TableConfig {
    pub fields: HashMap<&'static str, FieldConfig>,  // Use static str for zero-allocation
}

#[repr(align(64))]  // Align to cache line for better performance
pub struct Table {
    pub name: &'static str,  // Use static str
    pub field_configs: HashMap<&'static str, FieldConfig>,
    pub field_buffers: DashMap<&'static str, Arc<LowLatencyMpmcRing<Box<[u8]>>>>,
    pub record_count: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - 32],
}

impl Table {
    #[inline(always)]
    pub fn new(name: &'static str, config: TableConfig) -> Self {
        let mut table = Self {
            name,
            field_configs: HashMap::with_capacity(config.fields.len()),
            field_buffers: DashMap::with_capacity(config.fields.len()),
            record_count: AtomicUsize::new(0),
            _padding: [0; CACHE_LINE_SIZE - 32],
        };

        // Pre-allocate all buffers at once
        for (field_name, fc) in config.fields {
            let ring = Arc::new(LowLatencyMpmcRing::new(fc.ring_capacity));
            table.field_configs.insert(field_name, fc);
            table.field_buffers.insert(field_name, ring);
        }

        table
    }

    #[inline(always)]
    pub fn write_record(&self, record: HashMap<&'static str, Box<[u8]>>) -> bool {
        // Fast path: check capacity first
        if self.record_count.load(Ordering::Relaxed) >= self.capacity() {
            return false;
        }

        // Pre-check all buffers to avoid partial writes
        for (field_name, _) in &record {
            if let Some(ring_arc) = self.field_buffers.get(field_name) {
                if ring_arc.is_full() {
                    return false;
                }
            }
        }

        // All checks passed, perform the write
        for (field_name, data) in record {
            if let Some(ring_arc) = self.field_buffers.get(field_name) {
                if !ring_arc.try_enqueue(data) {
                    return false;
                }
            }
        }
        
        self.record_count.fetch_add(1, Ordering::Release);
        true
    }

    #[inline(always)]
    pub fn read_one_record(&self) -> Option<HashMap<&'static str, Box<[u8]>>> {
        // Fast path: check if empty
        if self.record_count.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let mut out = HashMap::with_capacity(self.field_buffers.len());
        
        // Pre-check all buffers to avoid partial reads
        for item in self.field_buffers.iter() {
            if item.value().is_empty() {
                return None;
            }
        }

        // All checks passed, perform the read
        for item in self.field_buffers.iter() {
            let field_name = *item.key();
            if let Some(bytes) = item.value().try_dequeue() {
                out.insert(field_name, bytes);
            } else {
                return None;
            }
        }

        self.record_count.fetch_sub(1, Ordering::Release);
        Some(out)
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.field_configs.values().next().map_or(0, |fc| fc.ring_capacity)
    }
} 