#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::key::{self, KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            return;
        }
        self.idx = 0;
        let first_off = self.block.offsets[0] as usize;
        self.seek_to_offset(first_off);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }
        let total = self.block.offsets.len();
        assert!(self.idx < total);
        self.idx += 1;
        if self.idx == total {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[self.idx] as usize;
        self.seek_to_offset(offset);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if self.block.offsets.is_empty() {
            return;
        }

        let mut low = 0 as usize;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = (low + high) / 2;
            let offset = self.block.offsets[mid] as usize;
            let mid_key = self.offset_to_key(offset);
            match mid_key.cmp(&key) {
                std::cmp::Ordering::Less => {
                    low = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    low = mid;
                    break;
                }
                std::cmp::Ordering::Greater => {
                    high = mid;
                }
            }
        }

        if low == self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[low] as usize;
        self.seek_to_offset(offset);
    }

    fn offset_to_key(&self, offset: usize) -> KeySlice {
        let key_len = (&self.block.data[offset..offset + SIZEOF_U16]).get_u16() as usize;
        let key_off = offset + SIZEOF_U16;
        key::KeySlice::from_slice(&self.block.data[key_off..key_off + key_len])
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let key_len = (&self.block.data[offset..offset + SIZEOF_U16]).get_u16() as usize;
        let key_off = offset + SIZEOF_U16;
        let key_vec = self.block.data[key_off..key_off + key_len].to_vec();
        self.key = KeyVec::from_vec(key_vec);
        let value_len = (&self.block.data[key_off + key_len..key_off + key_len + SIZEOF_U16])
            .get_u16() as usize;
        let value_start = key_off + key_len + SIZEOF_U16;
        let value_end = value_start + value_len;
        self.value_range = (value_start, value_end);
    }
}
