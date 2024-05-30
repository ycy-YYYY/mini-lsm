#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::KeySlice;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        debug_assert!(!key.is_empty());

        let entry_size = self.entry_size(key, value);
        if self.estimate_size() + entry_size > self.block_size && !self.is_empty() {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        let key_len: usize = key.len();
        let val_len = value.len();
        self.data.put_u16(key_len as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16(val_len as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn entry_size(&self, key: KeySlice, value: &[u8]) -> usize {
        // key length + value length + 2 bytes for key length + 2 bytes for value length
        key.len() + value.len() + SIZEOF_U16 /* key length */ + SIZEOF_U16 /* value length */
    }

    fn estimate_size(&self) -> usize {
        self.data.len()/* data size */ + self.offsets.len() /* offset size */ * SIZEOF_U16 + SIZEOF_U16
        /* num offset size */
    }
}
