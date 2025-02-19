use crate::page::Page;
use crate::page_header::PageHeader;
use crate::tx::{CommitReq, Tx};
use crate::types::{PageId, TxId};
use crate::{constant, page_header, utils};
use anyhow::Result;
use dashmap::DashMap;
use memmap2::{Advice, MmapMut, MmapOptions};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::{fs, thread};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{forget, ManuallyDrop};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::Ordering as atomic_ordering;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::sync::mpsc::{Receiver, SyncSender};

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;

pub(crate) const DB_HEADER_SIZE: usize = 100;
pub(crate) const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 * 1;
pub(crate) const DEFAULT_COMMIT_REQ_CHAN_BUFFER_SIZE: usize = 1000;

pub struct DB {
    dbHeaderMmap: MmapMut,
    lock: Mutex<()>,
    txIdCounter: AtomicU64,
    /// sorted by block file number
    blockFileFds: Vec<RawFd>,
    pageId2Page: DashMap<PageId, Arc<Page>>,
    pub(crate) memTable: RwLock<BTreeMap<Vec<u8>, Arc<Vec<u8>>>>,
    pub(crate) immutableMemTables: Vec<BTreeMap<Vec<u8>, Arc<Vec<u8>>>>,
    pub(crate) commitReqSender: SyncSender<CommitReq>,
}

// pub fn
impl DB {
    pub fn open(dbOption: Option<DBOption>) -> Result<Arc<DB>> {
        let dbOption = dbOption.unwrap_or(DBOption::default());

        Self::verifyDirPath(&dbOption.dirPath)?;

        let shouldInit =
            || -> Result<bool> {
                for dirEntry in fs::read_dir(&dbOption.dirPath)? {
                    _ = dirEntry?;
                    return Ok(false);
                }

                Ok(true)
            }()?;

        let block0FileFd =
            // generate dbHeader and initial some pages, write them into files
            if shouldInit {
                Self::init(&dbOption)?
            } else { // open the already exsit db files
                let block0FilePath = Path::join(dbOption.dirPath.as_ref(), "0.data");
                let block0File = OpenOptions::new().read(true).write(true).open(block0FilePath)?;

                if block0File.metadata()?.len() < DB_HEADER_SIZE as u64 {
                    throw!("0.data size should be greate than db DB_HEADER_SIZE");
                }

                let block0FileFd = block0File.as_raw_fd();
                forget(block0File);

                block0FileFd
            };

        // map 0.data leading part into dbHeader
        let dbHeaderMmap = unsafe {
            let dbHeaderMmap = {
                let mut mmapOptions = MmapOptions::new();
                mmapOptions.offset(0).len(DB_HEADER_SIZE);
                mmapOptions.map_mut(block0FileFd)?
            };

            dbHeaderMmap.advise(Advice::WillNeed)?;
            dbHeaderMmap.lock()?;

            dbHeaderMmap
        };

        let dbHeader = unsafe { utils::slice2RefMut::<DBHeader>(&dbHeaderMmap) };
        dbHeader.verify()?;

        // try to hold all data file fds
        let dataFds = {
            let mut blockFileFds = Vec::new();

            blockFileFds.push((0, block0FileFd));

            for readDir in fs::read_dir(&dbOption.dirPath)? {
                let readDir = readDir?;

                let path = readDir.path();

                if !path.ends_with(".data") {
                    continue;
                }

                let blockFileName = path.file_name().unwrap().to_str().unwrap();
                let elemVec = blockFileName.split(constant::DOT_STR).collect::<Vec<&str>>();
                if 1 >= elemVec.len() {
                    continue;
                }

                let blockNum = u64::from_str(elemVec.get(0).unwrap())?;

                if blockNum == 0 {
                    continue;
                }

                // block file size
                if readDir.metadata()?.len() != dbHeader.blockSize as u64 {
                    throw!("block file size should be equal to blockSize in dbHeader");
                }

                let blockFile = OpenOptions::new().read(true).write(true).open(path)?;
                blockFileFds.push((blockNum, blockFile.as_raw_fd()));

                let _ = ManuallyDrop::new(blockFile);
            }

            blockFileFds.sort_by(|a, b| { a.0.cmp(&b.0) });

            blockFileFds.into_iter().map(|x| x.1).collect::<Vec<_>>()
        };

        let (commitReqSender, commitReqReceiver) =
            mpsc::sync_channel::<CommitReq>(dbOption.commitReqChanBufferSize);

        let db = Arc::new(DB {
            dbHeaderMmap,
            lock: Mutex::new(()),
            txIdCounter: AtomicU64::new(dbHeader.lastTxId + 1),
            blockFileFds: dataFds,
            pageId2Page: DashMap::new(),
            memTable: Default::default(),
            immutableMemTables: Default::default(),
            commitReqSender,
        });

        let dbClone = db.clone();
        let _ = thread::Builder::new().name("thread_process_commit_reqs".to_string()).spawn(move || {
            dbClone.processCommitReqs(commitReqReceiver)
        });

        Ok(db)
    }

    pub fn newTx(self: &Arc<Self>) -> Result<Tx> {
        let txId = self.txIdCounter.fetch_add(1, atomic_ordering::SeqCst);

        let dbHeader = self.getHeaderMut();

        // txId increase
        {
            let lock = self.lock.lock().unwrap();
            dbHeader.lastTxId = txId;
            self.dbHeaderMmap.flush()?;
        }

        let tx = Tx {
            id: txId,
            db: self.clone(),
            changes: BTreeMap::new(),
            committed: AtomicBool::new(false),
        };

        Ok(tx)
    }
}

// pub (crate) fn
impl DB {
    #[inline]
    pub(crate) fn getHeader(&self) -> &DBHeader {
        unsafe { utils::slice2Ref(&self.dbHeaderMmap) }
    }

    #[inline]
    pub(crate) fn getHeaderMut(&self) -> &mut DBHeader {
        unsafe { utils::slice2RefMut(&self.dbHeaderMmap) }
    }

    pub(crate) fn getPageById(&self,
                              pageId: PageId,
                              parentPage: Option<Arc<Page>>) -> Result<Arc<Page>> {
        let dbHeader = self.getHeader();

        if let Some(page) = self.pageId2Page.get(&pageId) {
            return Ok(page.clone());
        }

        let targetBlockFd = {
            let blockNum = (dbHeader.pageSize as u64 * pageId) / dbHeader.blockSize as u64;
            self.blockFileFds.get(blockNum as usize).unwrap()
        };

        let pageMmap = {
            let pageHeaderOffsetInBlock = (dbHeader.pageSize as u64 * pageId) % dbHeader.blockSize as u64;
            utils::mmapFd(*targetBlockFd, pageHeaderOffsetInBlock, dbHeader.pageSize as usize)?
        };

        let mut page = Page::readFromPageHeader(pageMmap);
        page.parentPage = parentPage;

        let page = Arc::new(page);
        self.pageId2Page.insert(pageId, page.clone());

        Ok(page)
    }
}

// fn
impl DB {
    fn verifyDirPath(dirPath: impl AsRef<Path> + Display) -> Result<()> {
        if Path::exists(&dirPath.as_ref()) == false {
            fs::create_dir_all(&dirPath)?;
            return Ok(());
        }

        // // As lifts over &
        // #[stable(feature = "rust1", since = "1.0.0")]
        // impl<T: ?Sized, U: ?Sized> AsRef<U> for &T
        // where
        //     T: AsRef<U>,
        // {
        //     #[inline]
        //     fn as_ref(&self) -> &U {
        //         <T as AsRef<U>>::as_ref(*self)
        //     }
        // }
        let actualPath = utils::recursiveSymbolic(&dirPath)?;
        let metadata = fs::metadata(&actualPath)?;
        if metadata.is_file() {
            throw!(format!("{} actually is a file", dirPath));
        }

        if utils::haveWritePermission(&metadata) == false {
            throw!(format!("you have no write permission on {}", dirPath));
        }

        Ok(())
    }

    fn init(dbOption: &DBOption) -> Result<RawFd> {
        // allocate space for 2 init pages in memory
        let pageSize = utils::getOsPageSize();
        let mut pageSpace = vec![0; pageSize as usize * 2];

        // transmute as dbHeader in page0, write dbHeader field values
        let dbHeader: &mut DBHeader = unsafe { utils::slice2RefMut(&mut pageSpace) };
        dbHeader.magic = MAGIC;
        dbHeader.version = VERSION;
        dbHeader.pageSize = utils::getOsPageSize();
        dbHeader.blockSize = if dbOption.blockSize > 0 {
            dbOption.blockSize
        } else {
            DEFAULT_BLOCK_SIZE
        };
        dbHeader.lastTxId = 0;
        dbHeader.rootPageId = 1;

        // transmute as pageHeader in page0
        let page0Header: &mut PageHeader = unsafe { utils::slice2RefMut(&mut pageSpace[DB_HEADER_SIZE..]) };
        page0Header.pageId = 0;
        page0Header.flags = page_header::PAGE_FLAG_META;

        // transmute as pageHeader in page1
        let page0Header: &mut PageHeader = unsafe { utils::slice2RefMut(&mut pageSpace[pageSize as usize..]) };
        page0Header.pageId = 1;
        page0Header.flags = page_header::PAGE_FLAG_LEAF;

        // idea from bbolt
        let blockCount = (pageSpace.capacity() + dbHeader.blockSize as usize - 1) / dbHeader.blockSize as usize;

        let mut block0Fd = 0;
        for blockFileNum in 0..blockCount {
            let blockFilePath = Path::join(dbOption.dirPath.as_ref(), format!("{}.data", blockFileNum));
            let mut blockFile = OpenOptions::new().read(true).write(true).create_new(true).open(blockFilePath)?;

            // extend to blockSize
            blockFile.set_len(dbHeader.blockSize as u64)?;

            // last part
            let data2Write =
                if blockFileNum == blockCount - 1 {
                    &pageSpace[blockFileNum * dbHeader.blockSize as usize..]
                } else {
                    &pageSpace[blockFileNum * dbHeader.blockSize as usize..(blockFileNum + 1) * dbHeader.blockSize as usize]
                };

            let writtenSize = blockFile.write(data2Write)?;
            assert_eq!(writtenSize, data2Write.len());

            blockFile.sync_all()?;

            if blockFileNum == 0 {
                block0Fd = blockFile.as_raw_fd();
                forget(blockFile);
            }
        }

        Ok(block0Fd)
    }

    fn processCommitReqs(&self, commitReqReceiver: Receiver<CommitReq>) {
        // write changes in commitReq into memtable
        for commitReq in commitReqReceiver {
            commitReq.commitResultSender.send(Ok(())).unwrap()
        }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        for dataFd in self.blockFileFds.iter() {
            let file = unsafe { File::from_raw_fd(*dataFd) };
            drop(file);
        }
    }
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub(crate) struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: u16,
    pub blockSize: u32,
    pub lastTxId: TxId,
    pub rootPageId: PageId,
}

impl DBHeader {
    pub fn verify(&self) -> Result<()> {
        if self.magic != MAGIC {
            throw!("incorrect magic");
        }

        if self.version != VERSION {
            throw!("incorrect version");
        }

        // the db file may from another os,page size is different
        if utils::isPowerOfTwo(self.pageSize) == false {
            throw!("page size must be a power of two");
        }

        if utils::isPowerOfTwo(self.blockSize) == false {
            throw!("mmap unit size must be a power of two");
        }

        if self.pageSize as u32 > self.blockSize {
            throw!("page size should not exceed mmapUnitSize");
        }

        Ok(())
    }
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub(crate) struct BlockHeader;

pub struct DBOption {
    pub dirPath: String,
    pub blockSize: u32,
    pub commitReqChanBufferSize: usize,
}

impl Default for DBOption {
    fn default() -> Self {
        DBOption {
            dirPath: "./data".to_string(),
            blockSize: DEFAULT_BLOCK_SIZE,
            commitReqChanBufferSize: DEFAULT_COMMIT_REQ_CHAN_BUFFER_SIZE,
        }
    }
}
