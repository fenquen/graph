use anyhow::Result;
use memmap2::MmapMut;
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::os::fd::{FromRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// memTable and underlying data
pub(crate) struct MemTable {
    /// underlying data file fd
    memTableFileFd: RawFd,

    /// map whole data file
    memTableFileMmap: MmapMut,

    actions: BTreeMap<Vec<u8>, Option<Arc<Vec<u8>>>>,
}

impl MemTable {
    /// restore existed memTable file / create a new one
    pub(crate) fn open(memTableFilePath: impl AsRef<Path>) -> Result<MemTable> {
        

        todo!()
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

/// represenation in file <br>
/// keySize u16 | valSize u32 | key | val
#[repr(C)]
pub(crate) struct MemTableFileEntryHeader {
    pub(crate) keySize: u16,
    pub(crate) valSize: u32,
}

impl MemTableFileEntryHeader {
    #[inline]
    pub(crate) fn size(&self) -> usize {
        size_of::<MemTableFileEntryHeader>() + self.keySize as usize + self.valSize as usize
    }
}

