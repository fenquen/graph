use crate::tx::CommitReq;
use crate::{tx, utils};
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::BTreeMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::mem::forget;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::{Arc, Weak};
use crate::db::DB;

/// memTable and underlying data
pub(crate) struct MemTable {
    pub(crate) memTableFileNum: usize,

    /// underlying data file fd
    memTableFileFd: RawFd,

    /// map whole data file
    memTableFileMmap: MmapMut,

    pos: usize,
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Arc<Vec<u8>>>>,

    /// the reference to db
    pub(crate) db: Weak<DB>,
}

impl MemTable {
    /// restore existed memTable file / create a new one
    pub(crate) fn open(memTableFilePath: impl AsRef<Path>, newMemTableFileSize: usize) -> Result<MemTable> {
        let memTableFileNum = utils::extractFileNum(&memTableFilePath).unwrap();

        let alreadyExisted = fs::exists(&memTableFilePath)?;

        let mut openOptions = OpenOptions::new();
        openOptions.read(true).write(true).create(true);

        let memTableFile = openOptions.open(&memTableFilePath)?;

        // created a new onw
        if alreadyExisted == false {
            memTableFile.set_len(newMemTableFileSize as u64)?;
            memTableFile.sync_all()?;
        }

        let memTableFileFd = memTableFile.as_raw_fd();
        forget(memTableFile);

        let memTableFileMmap = utils::mmapMutFd(memTableFileFd, None, None)?;
        memTableFileMmap.advise(Advice::WillNeed)?;

        let mut memTable = MemTable {
            memTableFileNum,
            memTableFileFd,
            memTableFileMmap,
            pos: MEM_TABLE_FILE_HEADER_SIZE,
            changes: BTreeMap::new(),
            db: Weak::new(),
        };

        if alreadyExisted == false {
            return Ok(memTable);
        }

        // replay
        memTable.replay()?;

        Ok(memTable)
    }

    pub(crate) fn replay(&mut self) -> Result<()> {
        let memTableFileHeader: &MemTableFileHeader = self.getMemTableFileHeader();

        for _ in 0..memTableFileHeader.entryCount as usize {
            let memTableFileEntryHeader: &MemTableFileEntryHeader = utils::slice2Ref(&self.memTableFileMmap[self.pos..]);

            // keySize should greater than 0, valSize can be 0
            assert!(memTableFileEntryHeader.keySize > 0);

            let key = {
                let start = self.pos + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
                let end = start + memTableFileEntryHeader.keySize as usize;
                (&self.memTableFileMmap[start..end]).to_vec()
            };

            let val =
                if memTableFileEntryHeader.valSize > 0 {
                    let start = self.pos + MEM_TABLE_FILE_ENTRY_HEADER_SIZE + memTableFileEntryHeader.keySize as usize;
                    let end = start + memTableFileEntryHeader.valSize as usize;
                    Some(Arc::new((&self.memTableFileMmap[start..end]).to_vec()))
                } else {
                    None
                };

            self.changes.insert(key, val);

            self.pos += memTableFileEntryHeader.entrySize();
        }

        Ok(())
    }

    pub(crate) fn processCommitReq(&mut self, commitReq: CommitReq) {
        let process = || {
            let changeCount = commitReq.changes.len() as u32;

            for (keyWithoutTxId, val) in commitReq.changes {
                let keyWithTxId = tx::appendKeyWithTxId0(keyWithoutTxId, commitReq.txId);
                self.writeChange(keyWithTxId, val)?;
            }

            let memTableFileHeader = self.getMemTableFileHeaderMut();
            memTableFileHeader.entryCount += changeCount;
            self.memTableFileMmap.flush()?;

            anyhow::Ok(())
        };

        _ = commitReq.commitResultSender.send(process());
    }

    fn writeChange(&mut self, keyWithTxId: Vec<u8>, val: Option<Vec<u8>>) -> Result<()> {
        // write to file
        {
            let memTableFileEntryHeader = {
                let memTableFileEntryHeader: &mut MemTableFileEntryHeader = utils::slice2RefMut(&self.memTableFileMmap[self.pos..]);

                memTableFileEntryHeader.keySize = keyWithTxId.len() as u16;

                if let Some(ref val) = val {
                    memTableFileEntryHeader.valSize = val.len() as u32;
                }

                memTableFileEntryHeader
            };

            let entryContentMmap = {
                let start = self.pos + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
                let end = self.pos + memTableFileEntryHeader.entrySize();

                &mut self.memTableFileMmap[start..end]
            };

            // write key
            entryContentMmap[..keyWithTxId.len()].copy_from_slice(keyWithTxId.as_slice());

            // write val
            if let Some(ref val) = val {
                entryContentMmap[keyWithTxId.len()..].copy_from_slice(val);
            }

            self.pos += memTableFileEntryHeader.entrySize();
        }

        // write to map
        self.changes.insert(keyWithTxId, val.map(Arc::new));
        
        let db = self.db.upgrade().unwrap();
        
        // become immutable memTable
        if self.pos >= db.dbOption.memTableMaxSize {
           
        }

        Ok(())
    }

    #[inline]
    fn getMemTableFileHeader(&self) -> &MemTableFileHeader {
        utils::slice2Ref(&self.memTableFileMmap)
    }

    #[inline]
    fn getMemTableFileHeaderMut(&self) -> &mut MemTableFileHeader {
        utils::slice2RefMut(&self.memTableFileMmap)
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        // close fd before munmap not wrong
        let file = unsafe { File::from_raw_fd(self.memTableFileFd) };
        drop(file);
    }
}

pub(crate) const MEM_TABLE_FILE_HEADER_SIZE: usize = size_of::<MemTableFileHeader>();

#[repr(C)]
pub(crate) struct MemTableFileHeader {
    pub(crate) entryCount: u32,
}

pub(crate) const MEM_TABLE_FILE_ENTRY_HEADER_SIZE: usize = size_of::<MemTableFileEntryHeader>();

/// representation in file <br>
/// keySize u16 | valSize u32 | key | val
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MemTableFileEntryHeader {
    pub(crate) keySize: u16,
    pub(crate) valSize: u32,
}

impl MemTableFileEntryHeader {
    #[inline]
    pub(crate) fn entrySize(&self) -> usize {
        MEM_TABLE_FILE_ENTRY_HEADER_SIZE + self.keySize as usize + self.valSize as usize
    }
}

