use crate::tx::{CommitReq};
use crate::{page_header, tx, utils};
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::BTreeMap;
use std::{fs, mem, ptr};
use std::fs::{File, OpenOptions};
use std::mem::forget;
use std::ops::{DerefMut};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};
use crate::cursor::Cursor;
use crate::db::{DB, MEM_TABLE_FILE_EXTENSION};
use crate::page::Page;
use crate::page_elem::PageElem;
use crate::page_header::PageHeader;

/// memTable 文件结构
/// | entryCount | keySize | valSize | 实际data | keySize | valSize | 实际data |
pub(crate) struct MemTable {
    pub(crate) memTableFileNum: usize,

    /// 因为通过file struct 得不到对应的path,只能这里单独记录了
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

    pub(crate) header: &'static mut MemTableFileHeader,
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

        let header = utils::slice2RefMut(&memTableFileMmap);

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
            header,
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
            // let changeCount = commitReq.changes.len() as u32;

            // 原来的posInFile
            let _ = self.posInFile;

            for (keyWithoutTxId, val) in commitReq.changes {
                let keyWithTxId = tx::appendKeyWithTxId0(keyWithoutTxId, commitReq.txId);
                self.writeChange(keyWithTxId, val)?;
            }

            // todo 这里会有问题 当切换了memTable文件后 memTableFileHeader是新文件的entryCount不应该是changeCount的
            // let memTableFileHeader = self.getMemTableFileHeaderMut();
            // memTableFileHeader.entryCount += changeCount;

            // todo 系统调用msync成本很高
            // 减少调用的趟数 只同步变化部分
            self.msync()?;

            anyhow::Ok(())
        };

        _ = commitReq.commitResultSender.send(process());
    }

    /// protected by write mutex
    fn writeChange(self: &mut MemTable,
                   keyWithTxId: Vec<u8>, val: Option<Vec<u8>>) -> Result<()> {
        assert_eq!(self.immutable, false);

        let db = self.db.upgrade().unwrap();

        let switch2NewMemTable =
            |oldMemTable: &mut MemTable, newMemTableFileSize: usize| {
                // open a new memTable
                let mut newMemTable = {
                    let mutableMemTableFilePath =
                        Path::join(db.dbOption.dirPath.as_ref(),
                                   format!("{}.{}", oldMemTable.memTableFileNum + 1, MEM_TABLE_FILE_EXTENSION));

                    MemTable::open(mutableMemTableFilePath, newMemTableFileSize)?
                };

                newMemTable.db = Arc::downgrade(&db);

                // 使用新的替换旧的memeTable,换下来的旧的变为immutableMemTable收集
                // see RWLock::replace
                let memTableOld = mem::replace(oldMemTable, newMemTable);

                // 原来的oldMemTable被替换后 有必要msync的
                // 只mysnc必要的部分
                memTableOld.msync()?;

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

                    // 将变动落地到当前的内存中结构中
                    for (key, val) in changesTotal {
                        cursor.seek(key.as_slice(), true, val)?;
                    }

                    {
                        let mut involvedParentPages = Some(Vec::<Arc<RwLock<Page>>>::new());

                        let writePages =
                            |writeDestPage2IndexInParentPage: &[(Arc<RwLock<Page>>, usize)],
                             involvedParentPages: &mut Vec<Arc<RwLock<Page>>>| -> Result<()> {
                                for (page0, indexInParentPage) in writeDestPage2IndexInParentPage.iter() {
                                    let mut writeDestPage = page0.write().unwrap();
                                    let writeDestPage = writeDestPage.deref_mut();

                                    writeDestPage.write(&db)?;

                                    // 原来的单个的leaf 现在成了多个 需要原来的那个单个的leaf的parantPage来应对
                                    // 得要知道当前的这个page在父级的那个位置,然后在对应的位置塞入data
                                    // 例如 原来 这个leafPage在它的上级中是对应(700,750]的
                                    // 现在的话(700,750]这段区间又要分裂了
                                    // 原来是单单这1个区间对应1个leaf,现在是分成多个各自对应单个leaf的

                                    // 现在要知道各个分裂出来的leafPage的最大的key是多少
                                    // 要现在最底下的writeDestLeafPage层上平坦横向scan掉然后再到上级的

                                    // 读取当前的additionalPage的最大的key
                                    let getLargestKeyInPage = |page: &Page| -> Result<Vec<u8>> {
                                        let lastElemMeta = page.getLastElemMeta()?;
                                        match lastElemMeta.readPageElem() {
                                            PageElem::LeafR(k, _) => Ok(k.to_owned()),
                                            PageElem::Dummy4PutLeaf(k, _) => Ok(k.clone()),
                                            PageElem::LeafOverflowR(k, _) => Ok(k.to_owned()),
                                            PageElem::Dummy4PutLeafOverflow(k, _, _) => Ok(k.clone()),
                                            _ => panic!("impossible")
                                        }
                                    };

                                    let mut indexInParentPage = *indexInParentPage;

                                    let largestKeyInPage = getLargestKeyInPage(writeDestPage)?;

                                    // 意味着到了rootPage了
                                    if writeDestPage.parentPage.is_none() {
                                        // 当前的page塞不下了,需要新创建1个顶头的非叶子的root节点
                                        // 如果说rootPage还是容纳的下 那么不用去理会了
                                        if writeDestPage.additionalPages.len() > 0 {
                                            let mmapMut = db.allocateNewPage()?;

                                            let pageHeader: &mut PageHeader = utils::slice2RefMut(&mmapMut);
                                            pageHeader.flags = page_header::PAGE_FLAG_BRANCH;

                                            // 因为rootPage变动了,dbHeader的rootPageId也要相应的变化
                                            db.getHeaderMut().rootPageId = pageHeader.pageId;

                                            writeDestPage.parentPage = Some(Arc::new(RwLock::new(Page::readFromMmap(mmapMut)?)));

                                            let parentPage = writeDestPage.parentPage.as_ref().unwrap();

                                            // 添加之前先瞧瞧是不是已经有了相应的pageId了
                                            involvedParentPages.push(parentPage.clone());

                                            let mut parentPage = parentPage.write().unwrap();

                                            // 不应该使用insert,而是应直接替换的
                                            parentPage.pageElems[indexInParentPage] = PageElem::Dummy4PutBranch(largestKeyInPage, page0.clone());

                                            // 如果还有additionalPage的话,就要在indexInParentPage后边不断的塞入的
                                            for additionalPage in writeDestPage.additionalPages.iter() {
                                                indexInParentPage += 1;

                                                let largestKeyInPage = getLargestKeyInPage(additionalPage)?;
                                                parentPage.pageElems.insert(indexInParentPage, PageElem::Dummy4PutBranch0(largestKeyInPage, additionalPage.header.pageId))
                                            }
                                        }
                                    }
                                }

                                Ok(())
                            };

                        writePages(cursor.writeDestLeafPages.as_ref(), involvedParentPages.as_mut().unwrap())?;

                        loop {
                            let involvedParentPagesPrevRound = involvedParentPages.replace(Vec::new()).unwrap();
                            if involvedParentPagesPrevRound.is_empty() {
                                break;
                            }

                            let page2IndexInParentPage = involvedParentPagesPrevRound.into_iter().map(|page| {
                                let indexInParentPage = {
                                    let pageReadGuard = page.read().unwrap();
                                    pageReadGuard.indexInParentPage.unwrap()
                                };

                                (page, indexInParentPage)
                            }).collect::<Vec<_>>();

                            writePages(&page2IndexInParentPage, involvedParentPages.as_mut().unwrap())?;
                        }
                    }

                    // immutalbleMemTables因为数量满了持久化后需要清理掉对应已经无用的文件的
                    for immutableMemTable in immutableMemTables.drain(..) {
                        let immutableMemTableFilePath = immutableMemTable.memTableFilePath.clone();
                        drop(immutableMemTable);
                        fs::remove_file(&immutableMemTableFilePath)?;
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
                switch2NewMemTable(self, db.dbOption.memTableMaxSize + MEM_TABLE_FILE_HEADER_SIZE + entryTotalSize)?;
            }

            // 当前这个的kv对应的memtableEnrty
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

            // 落地 key 到memtable对应的文件
            entryContentMmap[..keyWithTxId.len()].copy_from_slice(keyWithTxId.as_slice());

            // 落地 val 到memtable对应的文件
            if let Some(ref val) = val {
                entryContentMmap[keyWithTxId.len()..].copy_from_slice(val);
            }

            self.header.entryCount += 1;

            self.posInFile += memTableFileEntryHeader.entrySize();
        }

        // 变量体系中同步记录
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

        drop(self);
        fs::remove_file(memTableFilePath)?;

        Ok(())
    }

    #[inline]
    pub(crate) fn msync(&self) -> Result<()> {
        self.memTableFileMmap.flush_range(0, self.posInFile)?;
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
        MEM_TABLE_FILE_ENTRY_HEADER_SIZE +
            self.keySize as usize + self.valSize as usize
    }
}

