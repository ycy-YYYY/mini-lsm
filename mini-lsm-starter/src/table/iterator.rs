#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: Option<BlockIterator>,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        Ok(Self {
            table,
            blk_iter: Some(BlockIterator::create_and_seek_to_first(block)),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            table: Arc::clone(&table),
            blk_iter: None,
            blk_idx: table.block_meta.len(),
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = self.table.find_block_idx(key);
        if self.blk_idx >= self.table.block_meta.len() {
            return Ok(());
        }
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
        while self.blk_iter.as_ref().unwrap().is_valid()
            && self.blk_iter.as_ref().unwrap().key() < key
        {
            self.blk_iter.as_mut().unwrap().next();
        }
        assert!(self.blk_iter.as_ref().unwrap().is_valid());
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        match &self.blk_iter {
            Some(iter) => iter.key(),
            None => KeySlice::default(),
        }
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        match &self.blk_iter {
            Some(iter) => iter.value(),
            None => &[],
        }
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        match &self.blk_iter {
            Some(iter) => iter.is_valid(),
            None => false,
        }
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.blk_iter {
            iter.next();
            if !iter.is_valid() {
                if self.blk_idx + 1 < self.table.block_meta.len() {
                    self.blk_idx += 1;
                    let block = self.table.read_block_cached(self.blk_idx)?;
                    self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
                }
            }
        }
        Ok(())
    }
}
