#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        assert!(!key.is_empty());
        if !self.builder.add(key, value) {
            self.finish_build_block();
            let _ = self.builder.add(key, value);
        }
        self.last_key = key.raw_ref().to_vec();
        if self.first_key.is_empty() {
            self.first_key = self.last_key.clone();
        }
    }

    fn finish_build_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let mut data = builder.build().encode().to_vec();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.as_slice())),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.last_key.as_slice())),
        });
        self.data.append(&mut data);
        self.first_key = Vec::new();
        self.last_key = Vec::new();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.block_size * self.meta.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_build_block();
        assert!(!self.meta.is_empty());
        let mut buf = self.data;
        let meta_offset = buf.len();
        let first_key = self.meta.first().unwrap().first_key.raw_ref();
        let last_key = self.meta.last().unwrap().last_key.raw_ref();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_meta: self.meta.clone(),
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
