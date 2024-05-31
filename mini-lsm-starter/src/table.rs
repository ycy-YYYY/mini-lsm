#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use anyhow::anyhow;
use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::{Block, SIZEOF_U16};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = 0;
        let original_len = buf.len();
        for meta in block_meta {
            // The size of offset
            estimated_size += std::mem::size_of::<u32>();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }
        buf.reserve(estimated_size);

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }

        assert!(estimated_size == buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            block_meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // read the last 4 bytes of the file to get the offset of block meta
        let meta_offset = file
            .read(
                file.size() - size_of::<u32>() as u64,
                size_of::<u32>() as u64,
            )?
            .as_slice()
            .get_u32() as usize;
        assert!(meta_offset < file.size() as usize);
        let meta_data = file.read(
            meta_offset as u64,
            file.size() - meta_offset as u64 - size_of::<u32>() as u64,
        )?;
        let block_meta = BlockMeta::decode_block_meta(meta_data.as_slice());
        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(block_idx < self.block_meta.len());
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(block_idx < self.block_meta.len());
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// if can not find a key >= keySlice, return sstable len
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut idx = self
            .block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key);

        if idx > 0 && self.block_meta[idx - 1].last_key.as_key_slice() >= key {
            idx -= 1;
        }

        idx
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
