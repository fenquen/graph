use crate::utils;
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::BTreeMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::mem::forget;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;

/// memTable and underlying data
pub(crate) struct MemTable {
    pub(crate) memTableFileNum: usize,

    /// underlying data file fd
    memTableFileFd: RawFd,

    /// map whole data file
    memTableFileMmap: MmapMut,

    pub(crate) actions: BTreeMap<Vec<u8>, Option<Arc<Vec<u8>>>>,
}

// pub (crate) fn
impl MemTable {
    pub(crate) fn get() {}

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
            actions: BTreeMap::new(),
        };

        if alreadyExisted == false {
            return Ok(memTable);
        }

        // replay
        memTable.replay()?;

        Ok(memTable)
    }

    pub(crate) fn replay(&mut self) -> Result<()> {
        let mut pos = 0usize;

        let memTableFileHeader: &MemTableFileHeader = utils::slice2Ref(self.memTableFileMmap.as_ref());

        pos += MEM_TABLE_FILE_HEADER_SIZE;

        for _ in 0..memTableFileHeader.entryCount as usize {
            let memTableFileEntryHeader: &MemTableFileEntryHeader = utils::slice2Ref(&self.memTableFileMmap[pos..]);

            // keySize should greater than 0, valSize can be 0
            assert!(memTableFileEntryHeader.keySize > 0);

            let key = {
                let start = pos + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
                let end = start + memTableFileEntryHeader.keySize as usize;
                (&self.memTableFileMmap[start..end]).to_vec()
            };

            let val =
                if memTableFileEntryHeader.valSize > 0 {
                    let start = pos + MEM_TABLE_FILE_ENTRY_HEADER_SIZE + memTableFileEntryHeader.keySize as usize;
                    let end = start + memTableFileEntryHeader.valSize as usize;
                    Some(Arc::new((&self.memTableFileMmap[start..end]).to_vec()))
                } else {
                    None
                };

            self.actions.insert(key, val);

            pos += memTableFileEntryHeader.memTableFileEntrySize();
        }

        Ok(())
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        // close fd before munmap not wrong
        let file = unsafe { File::from_raw_fd(self.memTableFileFd) };
        drop(file);
    }
}

#[repr(C)]
pub(crate) struct MemTableFileHeader {
    pub(crate) entryCount: u32,
}

pub(crate) const MEM_TABLE_FILE_HEADER_SIZE: usize = size_of::<MemTableFileHeader>();

/// represenation in file <br>
/// keySize u16 | valSize u32 | key | val
#[repr(C)]
pub(crate) struct MemTableFileEntryHeader {
    pub(crate) keySize: u16,
    pub(crate) valSize: u32,
}

pub(crate) const MEM_TABLE_FILE_ENTRY_HEADER_SIZE: usize = size_of::<MemTableFileEntryHeader>();

impl MemTableFileEntryHeader {
    #[inline]
    pub(crate) fn memTableFileEntrySize(&self) -> usize {
        MEM_TABLE_FILE_ENTRY_HEADER_SIZE + self.keySize as usize + self.valSize as usize
    }
}

