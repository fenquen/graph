use crate::tx::{CommitReq, Tx};
use crate::{tx, utils};
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::BTreeMap;
use std::{fs, mem, ptr};
use std::fs::{File, OpenOptions};
use std::mem::forget;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use crate::cursor::Cursor;
use crate::db::{DB, MEM_TABLE_FILE_EXTENSION};

/// memTable 文件结构
/// | entryCount | keySize | valSize | 实际data | keySize | valSize | 实际data |
pub(crate) struct MemTable {
    pub(crate) memTableFileNum: usize,

    /// so idiot, can not get path by file struct
    memTableFilePath: PathBuf,

    /// underlying data file fd
    memTableFileFd: RawFd,

    /// map whole data file
    memTableFileMmap: MmapMut,

    posInFile: usize,
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,

    /// db本身会持有MemTable,需要使用weak的
    pub(crate) db: Weak<DB>,

    pub(crate) immutable: bool,
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

        let memTableFileMmap = utils::mmapFdMut(memTableFileFd, None, None)?;
        memTableFileMmap.advise(Advice::WillNeed)?;

        let mut memTable = MemTable {
            memTableFileNum,
            memTableFilePath: memTableFilePath.as_ref().to_owned(),
            memTableFileFd,
            memTableFileMmap,
            posInFile: MEM_TABLE_FILE_HEADER_SIZE,
            changes: BTreeMap::new(),
            db: Weak::new(),
            // if the file already exist, then it is immutable
            immutable: alreadyExisted,
        };

        if alreadyExisted == false {
            return Ok(memTable);
        }

        // replay
        memTable.replay()?;

        Ok(memTable)
    }

    /// 回放memTable,memTable其实也起到了wal用途
    pub(crate) fn replay(&mut self) -> Result<()> {
        assert!(self.immutable);

        let memTableFileHeader: &MemTableFileHeader = self.getMemTableFileHeader();

        for _ in 0..memTableFileHeader.entryCount as usize {
            let memTableFileEntryHeader: &MemTableFileEntryHeader = utils::slice2Ref(&self.memTableFileMmap[self.posInFile..]);

            // keySize should greater than 0, valSize can be 0
            assert!(memTableFileEntryHeader.keySize > 0);

            let key = {
                let start = self.posInFile + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
                let end = start + memTableFileEntryHeader.keySize as usize;
                (&self.memTableFileMmap[start..end]).to_vec()
            };

            let val =
                if memTableFileEntryHeader.valSize > 0 {
                    let start = self.posInFile + MEM_TABLE_FILE_ENTRY_HEADER_SIZE + memTableFileEntryHeader.keySize as usize;
                    let end = start + memTableFileEntryHeader.valSize as usize;
                    Some((&self.memTableFileMmap[start..end]).to_vec())
                } else {
                    None
                };

            self.changes.insert(key, val);

            self.posInFile += memTableFileEntryHeader.entrySize();
        }

        Ok(())
    }

    /// protected by write mutex
    pub(crate) fn processCommitReq(&mut self, commitReq: CommitReq) {
        assert_eq!(self.immutable, false);

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

    /// protected by write mutex
    fn writeChange(self: &mut Self,
                   keyWithTxId: Vec<u8>, val: Option<Vec<u8>>) -> Result<()> {
        assert_eq!(self.immutable, false);

        let db = self.db.upgrade().unwrap();

        let switchToNewMemTable = |oldMemTable: &mut MemTable, newMemTableFileSize: usize| {
            // open a new memTable
            let mut newMemTable = {
                let mutableMemTableFilePath =
                    Path::join(db.dbOption.dirPath.as_ref(), format!("{}.{}", oldMemTable.memTableFileNum + 1, MEM_TABLE_FILE_EXTENSION));

                MemTable::open(mutableMemTableFilePath, newMemTableFileSize)?
            };

            newMemTable.db = Arc::downgrade(&db);

            // replace the old with the new one
            // see RWLock::replace
            let memTableOld = mem::replace(oldMemTable, newMemTable);

            // move the old memTable to immutableMemTables
            let mut immutableMemTables = db.immutableMemTables.write().unwrap();
            immutableMemTables.push(memTableOld);

            // 如果落地的memTable文件达到数量了,需要将现有的全部memTable文件内容落地到db的数据文件的
            if immutableMemTables.len() > db.dbOption.immutableMemTableCount {
                // immutableMemTables are in order from the oldest to the latest
                // merge changes across immutableMemTables

                let mut changesTotal = BTreeMap::new();

                // 遍历各个immutableMemTable 将其changes偷梁换柱替换的
                for immutableMemTable in immutableMemTables.iter() {
                    // here we can not move out changes, because memTable implenments Drop
                    // https://doc.rust-lang.org/error_codes/E0509.html
                    /*for (key, val) in immutableMemTable.changes {
                        cursor.put(key, val)?;
                    }*/

                    // 使用replace偷换出需要的内容,减少不必要的clone的
                    let changes: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
                        unsafe {
                            ptr::replace(&immutableMemTable.changes as *const BTreeMap<_, _> as _, BTreeMap::<_, _>::new())
                        };

                    for (key, val) in changes {
                        changesTotal.insert(key, val);
                    }
                }

                let mut cursor = Cursor::new(db.clone(), None)?;

                for (key, val) in changesTotal {
                    // 将变动落地到当前的内存中结构中
                    cursor.seek(key.as_slice(), true, val)?;
                }
            }

            anyhow::Ok(())
        };

        // write to file
        {
            let entryTotalSize = {
                let mut totalSize = MEM_TABLE_FILE_ENTRY_HEADER_SIZE;

                totalSize += keyWithTxId.len();

                if let Some(ref val) = val {
                    totalSize += val.len();
                }

                totalSize
            };

            // should move the current one to immutableMemTables then build a new one
            if self.posInFile + entryTotalSize >= self.memTableFileMmap.len() {
                switchToNewMemTable(self, db.dbOption.memTableMaxSize + MEM_TABLE_FILE_HEADER_SIZE + entryTotalSize)?;
            }

            let memTableFileEntryHeader = {
                let memTableFileEntryHeader: &mut MemTableFileEntryHeader =
                    utils::slice2RefMut(&self.memTableFileMmap[self.posInFile..]);

                memTableFileEntryHeader.keySize = keyWithTxId.len() as u16;

                if let Some(ref val) = val {
                    memTableFileEntryHeader.valSize = val.len() as u32;
                }

                memTableFileEntryHeader
            };

            let entryContentMmap = {
                let start = self.posInFile + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
                let end = self.posInFile + memTableFileEntryHeader.entrySize();

                &mut self.memTableFileMmap[start..end]
            };

            // write key
            entryContentMmap[..keyWithTxId.len()].copy_from_slice(keyWithTxId.as_slice());

            // write val
            if let Some(ref val) = val {
                entryContentMmap[keyWithTxId.len()..].copy_from_slice(val);
            }

            self.posInFile += memTableFileEntryHeader.entrySize();
        }

        // write to map
        self.changes.insert(keyWithTxId, val);

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

    fn destory(self) -> Result<()> {
        let memTableFilePath = self.memTableFilePath.clone();

        mem::drop(self);
        fs::remove_file(memTableFilePath)?;

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

