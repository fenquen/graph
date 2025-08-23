use crate::mem_table::MemTable;
use crate::page::Page;
use crate::page_header::PageHeader;
use crate::tx::{CommitReq, Tx};
use crate::types::{PageId, TxId};
use crate::{mem_table_r, page_header, utils};
use anyhow::Result;
use dashmap::DashMap;
use memmap2::{Advice, MmapMut};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as atomic_ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{mpsc, Arc, Mutex, RwLock, RwLockWriteGuard, Weak};
use std::{fs, mem, thread, u64, usize};
use std::mem::MaybeUninit;
use std::thread::JoinHandle;
use crate::mem_table_r::MemTableR;
use crate::utils::DEFAULT_PAGE_SIZE;

const MAGIC: usize = 0xCAFEBABE;
const VERSION: usize = 1;

pub(crate) const DB_HEADER_SIZE: usize = size_of::<DBHeader>();

pub(crate) const DEFAULT_DIR_PATH: &str = "./data";
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024;
pub(crate) const DEFAULT_COMMIT_REQ_CHAN_BUFFER_SIZE: usize = 1024;
pub(crate) const DEFAULT_MEM_TABLE_R_CHAN_BUFFER_SIZE: usize = 1024;
pub(crate) const DEFAULT_MEM_TABLE_MAX_SIZE: usize = 1024;
pub(crate) const DEFAULT_IMMUTABLE_MEM_TABLE_COUNT: usize = 1;

pub(crate) const BLOCK_FILE_EXTENSION: &str = "block";
pub(crate) const FIRST_BLOCK_FILE_NAME: &str = "0.block";
pub(crate) const MEM_TABLE_FILE_EXTENSION: &str = "mem";

pub struct DBOption {
    pub dirPath: String,

    pub commitReqChanBufSize: usize,

    pub memTableRChanBufSize: usize,

    /// 单纯的memTable的entry内容的最大多少,不含memTableFileHeader的
    pub memTableMaxSize: usize,

    /// how many immutable memTables to hold
    pub immutableMemTableCount: usize,

    /// once exceeded, combine
    pub pageMaxFreePercent: f64,

    pub pageFillPercentAfterSplit: f64,

    /// 和blockSize相同,只有初始化db时候有用,初始化之后db的pageSize是变不了的
    pub pageSize: usize,

    /// used only when init
    pub blockSize: usize,
}

impl Default for DBOption {
    fn default() -> DBOption {
        DBOption {
            dirPath: DEFAULT_DIR_PATH.to_string(),
            commitReqChanBufSize: DEFAULT_COMMIT_REQ_CHAN_BUFFER_SIZE,
            memTableRChanBufSize: DEFAULT_MEM_TABLE_R_CHAN_BUFFER_SIZE,
            memTableMaxSize: DEFAULT_MEM_TABLE_MAX_SIZE,
            immutableMemTableCount: DEFAULT_IMMUTABLE_MEM_TABLE_COUNT,
            pageMaxFreePercent: 0.0,
            pageFillPercentAfterSplit: 0.7,
            pageSize: DEFAULT_PAGE_SIZE,
            blockSize: DEFAULT_BLOCK_SIZE,
        }
    }
}

pub struct DB {
    pub(crate) dbOption: DBOption,

    dbHeaderMmap: MmapMut,
    lock: Mutex<()>,
    txIdCounter: AtomicU64,
    pageIdCounter: AtomicU64,

    /// sorted by block file number
    blockFileFds: RwLock<Vec<RawFd>>,

    pageId2Page: DashMap<PageId, Arc<RwLock<Page>>>,

    pub(crate) memTable: RwLock<MemTable>,
    pub(crate) immutableMemTables: RwLock<Vec<MemTable>>,

    pub(crate) commitReqSender: SyncSender<CommitReq>,
    pub(crate) memTableRSender: SyncSender<MemTableR>,

    pub(crate) joinHandleCommitReqs: MaybeUninit<JoinHandle<()>>,
    pub(crate) joinHandleMemTableRs: MaybeUninit<JoinHandle<()>>,

    pub(crate) flyingTxIds: RwLock<BTreeSet<TxId>>,

    pub(crate) availablePageSizeAfterSplit: usize,
}

impl DB {
    pub fn open(dbOption: Option<DBOption>) -> Result<Arc<DB>> {
        let mut dbOption = dbOption.unwrap_or(DBOption::default());

        DB::verifyDirPath(&dbOption.dirPath)?;

        let shouldInit =
            || -> Result<bool> {
                for dirEntry in fs::read_dir(&dbOption.dirPath)? {
                    _ = dirEntry?;
                    return Ok(false);
                }

                Ok(true)
            }()?;


        // generate dbHeader and initial some pages, write them into block files
        if shouldInit {
            DB::init(&dbOption)?
        }

        // mmap dbHeader in block0
        let dbHeaderMmap = {
            let block0FilePath = Path::join(dbOption.dirPath.as_ref(), FIRST_BLOCK_FILE_NAME);
            let block0File = OpenOptions::new().read(true).write(true).open(block0FilePath)?;

            if block0File.metadata()?.len() < DB_HEADER_SIZE as u64 {
                throw!("0.data size should be greater than db DB_HEADER_SIZE");
            }

            let block0FileFd = block0File.as_raw_fd();

            // 这里就算后边block0FileFd close了也是不会影响的
            let dbHeaderMmap = utils::mmapFdMut(block0FileFd, None, Some(DB_HEADER_SIZE))?;

            dbHeaderMmap.advise(Advice::WillNeed)?;
            dbHeaderMmap.lock()?;

            dbHeaderMmap
        };

        let dbHeader = utils::slice2RefMut::<DBHeader>(&dbHeaderMmap);
        dbHeader.verify()?;

        if shouldInit { // init时候会生成pageId是0和1的两个page的
            dbHeader.lastPageId = 1;
        }

        // memTableMaxSize要是实际的pageSize整数
        dbOption.memTableMaxSize = utils::roundUp2Multiple(dbOption.memTableMaxSize, dbHeader.pageSize);

        // 启动的时候将已经存在的memTable文件都视为immutable的
        let (blockFileFds, immutableMemTables) = DB::scanDir(dbHeader, &dbOption)?;

        // 当启动的时候总是会新生成1个作为当前的immutableMemTable
        let memTable = {
            let mutableMemTableFileNum =
                immutableMemTables.last().map_or_else(|| 0, |m| m.memTableFileNum + 1);

            let mutableMemTableFilePath =
                Path::join(dbOption.dirPath.as_ref(), format!("{}.{}", mutableMemTableFileNum, MEM_TABLE_FILE_EXTENSION));

            MemTable::open(mutableMemTableFilePath, dbOption.memTableMaxSize)?
        };

        let (commitReqSender, commitReqReceiver) =
            mpsc::sync_channel::<CommitReq>(dbOption.commitReqChanBufSize);

        let (memTableRSender, memTableRReceiver) =
            mpsc::sync_channel::<MemTableR>(dbOption.memTableRChanBufSize);

        let availablePageSizeAfterSplit = f64::ceil(dbHeader.pageSize as f64 * dbOption.pageFillPercentAfterSplit) as usize;
        
        let db =
            Arc::new(
                DB {
                    dbOption,
                    dbHeaderMmap,
                    lock: Mutex::new(()),
                    // 接着原来的保存在dbHeader的txId
                    txIdCounter: AtomicU64::new(dbHeader.lastTxId + 1),
                    pageIdCounter: AtomicU64::new(dbHeader.lastPageId + 1),
                    blockFileFds: RwLock::new(blockFileFds),
                    pageId2Page: DashMap::new(),
                    memTable: RwLock::new(memTable),
                    immutableMemTables: RwLock::new(immutableMemTables),
                    commitReqSender,
                    memTableRSender,
                    joinHandleCommitReqs: MaybeUninit::<JoinHandle<()>>::uninit(),
                    joinHandleMemTableRs: MaybeUninit::<JoinHandle<()>>::uninit(),
                    flyingTxIds: Default::default(),
                    availablePageSizeAfterSplit,
                }
            );

        // set reference to db
        {
            {
                let mut memTable = db.memTable.write().unwrap();
                memTable.db = Arc::downgrade(&db);
            }

            {
                let mut immutableMemTables = db.immutableMemTables.write().unwrap();
                for memTable in immutableMemTables.iter_mut() {
                    memTable.db = Arc::downgrade(&db);
                }
            }
        }

        // 事务提交处理的thread
        let dbClone = Arc::downgrade(&db);
        let joinHandleCommitReqs =
            thread::Builder::new().name("thread_process_commit_reqs".to_string()).spawn(move || {
                DB::processCommitReqs(dbClone, commitReqReceiver);
            })?;

        // 落地immutableMemTable的thread
        let dbClone = Arc::downgrade(&db);
        let a = db.dbOption.immutableMemTableCount;
        let joinHandleMemTableRs =
            thread::Builder::new().name("thread_process_mem_table_rs".to_string()).spawn(move || {
                DB::processMemTableRs(dbClone, memTableRReceiver, a);
            })?;

        // 使用非正常的手段设置
        {
            let db = unsafe { &mut *{ Arc::as_ptr(&db) as *mut DB } };
            db.joinHandleCommitReqs.write(joinHandleCommitReqs);
            db.joinHandleMemTableRs.write(joinHandleMemTableRs);
        }

        // 当前的这些immutableMemTables需要发送到对应的处理线程去落地
        {
            let immutableMemTables = db.immutableMemTables.read().unwrap();
            immutableMemTables.iter().for_each(|immutableMemTable| {
                let memTableR = MemTableR::try_from(immutableMemTable.memTableFileFd).unwrap();
                db.memTableRSender.send(memTableR).unwrap();
            });
        }

        Ok(db)
    }

    pub fn newTx(&'_ self) -> Result<Tx<'_>> {
        let txId = self.allocateTxId()?;

        {
            let mut infightingTxIds = self.flyingTxIds.write().unwrap();
            infightingTxIds.insert(txId);
        }

        let tx = Tx {
            id: txId,
            db: self,
            changes: BTreeMap::new(),
            committed: AtomicBool::new(false),
        };

        Ok(tx)
    }

    #[inline]
    pub(crate) fn getHeader(&self) -> &DBHeader {
        utils::slice2Ref(&self.dbHeaderMmap)
    }

    #[inline]
    pub(crate) fn getHeaderMut(&self) -> &mut DBHeader {
        utils::slice2RefMut(&self.dbHeaderMmap)
    }

    // todo 完成 感觉函数这样的传参数相当有问题,非得要外部已经知道了它的parentPage是谁了 其实parentPage是哪个应该是page固有信息的
    pub(crate) fn getPageById(&self, pageId: PageId) -> Result<Arc<RwLock<Page>>> {
        let dbHeader = self.getHeader();

        if let Some(page) = self.pageId2Page.get(&pageId) {
            return Ok(page.clone());
        }

        // todo 需要java的获取单例的double check套路
        let targetBlockFileFd = {
            let blockNum = (dbHeader.pageSize as u64 * pageId) / dbHeader.blockSize as u64;

            let blockFileFds = self.blockFileFds.read().unwrap();
            blockFileFds.get(blockNum as usize).unwrap().clone()
        };

        let pageMmapMut = {
            let pageHeaderOffsetInBlock = (dbHeader.pageSize as u64 * pageId) % dbHeader.blockSize as u64;
            utils::mmapFdMut(targetBlockFileFd, Some(pageHeaderOffsetInBlock), Some(dbHeader.pageSize))?
        };

        let mut page = Page::try_from(pageMmapMut)?;
        //page.parentPage = parentPage;

        let page = Arc::new(RwLock::new(page));
        self.pageId2Page.insert(pageId, page.clone());

        Ok(page)
    }

    pub(crate) fn allocateTxId(&self) -> Result<TxId> {
        let txId = self.txIdCounter.fetch_add(1, atomic_ordering::SeqCst);

        let dbHeader = self.getHeaderMut();

        // txId increase
        {
            let lock = self.lock.lock().unwrap();
            dbHeader.lastTxId = txId;
            self.dbHeaderMmap.flush()?;
        }

        Ok(txId)
    }

    pub(crate) fn allocateNewPage(&self, flags: u16) -> Result<Page> {
        // 需要blockSize 和 pageSize
        let dbHeader = self.getHeader();

        let pageId = self.allocatePageId()?;

        // 得到这个page应该是在哪个blockFile
        // blockFile可能需新建也能依然有了
        let blockFileNum = (dbHeader.pageSize as usize * pageId as usize) / dbHeader.blockSize as usize;

        let mut blockFileFds = self.blockFileFds.write().unwrap();

        // blockFile尚未创建
        if blockFileNum >= blockFileFds.len() {
            let blockFile = DB::generateBlockFile(&self.dbOption, blockFileNum, self.getHeader().blockSize)?;
            blockFileFds.push(blockFile.as_raw_fd());
            mem::forget(blockFile);
        }

        let blockFileFds = RwLockWriteGuard::downgrade(blockFileFds);
        let targetBlockFileFd = blockFileFds.get(blockFileNum).unwrap().clone();

        let mut pageMmap = {
            let pageHeaderOffsetInBlock = (dbHeader.pageSize as u64 * pageId) % dbHeader.blockSize as u64;
            utils::mmapFdMut(targetBlockFileFd, Some(pageHeaderOffsetInBlock), Some(dbHeader.pageSize))?
        };

        let pageHeader: &mut PageHeader = utils::slice2RefMut(&mut pageMmap);
        pageHeader.id = pageId;
        pageHeader.flags = flags;

        Ok(Page {
            //parentPage: None,
           // indexInParentPage: None,
            mmapMut: pageMmap,
            header: pageHeader,
            pageElems: vec![],
            //keyMin: Default::default(),
            //keyMax: Default::default(),
            //childPages: None,
            additionalPages: vec![],
        })
    }

    fn allocatePageId(&self) -> Result<PageId> {
        let pageId = self.pageIdCounter.fetch_add(1, atomic_ordering::SeqCst);

        let dbHeader = self.getHeaderMut();

        // pageId increase
        {
            let lock = self.lock.lock().unwrap();
            dbHeader.lastPageId = pageId;
            self.dbHeaderMmap.flush()?;
        }

        Ok(pageId)
    }

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

    fn init(dbOption: &DBOption) -> Result<()> {
        // 确保用户自定义的pageSize是os的pageSize整数
        let pageSize = utils::roundUp2Multiple(dbOption.pageSize, utils::getOsPageSize());

        // allocate space for 2 init pages in memory
        // 分别是容纳dbHeader的以及第1个leafPage
        let mut first2PageSpace = vec![0; pageSize * 2];

        // transmute as dbHeader in page0, write dbHeader field values
        let dbHeader: &mut DBHeader = utils::slice2RefMut(&mut first2PageSpace);
        dbHeader.magic = MAGIC;
        dbHeader.version = VERSION;
        dbHeader.pageSize = pageSize;
        // 如果blockSize不是pageSize整数倍的话,增加到那样大的
        dbHeader.blockSize = utils::roundUp2Multiple(dbOption.blockSize, dbHeader.pageSize);
        dbHeader.lastTxId = 0;
        dbHeader.rootPageId = 1;

        dbHeader.verify()?;

        // transmute as pageHeader in page0
        let page0Header: &mut PageHeader = utils::slice2RefMut(&mut first2PageSpace[DB_HEADER_SIZE..]);
        page0Header.id = 0;
        page0Header.flags = page_header::PAGE_FLAG_META;

        // transmute as pageHeader in page1
        let page1Header: &mut PageHeader = utils::slice2RefMut(&mut first2PageSpace[pageSize as usize..]);
        page1Header.id = 1;
        page1Header.flags = page_header::PAGE_FLAG_LEAF;

        // idea from bbolt,用来计算占用的数量而不是对应的block的index的
        let blockCount = ((pageSize * 2) as usize + dbHeader.blockSize as usize - 1) / dbHeader.blockSize as usize;

        // 生成各个需要的block对应文件
        for blockFileNum in 0..blockCount {
            let mut blockFile = DB::generateBlockFile(dbOption, blockFileNum, dbHeader.blockSize)?;

            // extend to blockSize
            blockFile.set_len(dbHeader.blockSize as u64)?;

            let data2Write =
                // last part
                if blockFileNum == blockCount - 1 {
                    &first2PageSpace[blockFileNum * dbHeader.blockSize as usize..]
                } else {
                    &first2PageSpace[blockFileNum * dbHeader.blockSize as usize..(blockFileNum + 1) * dbHeader.blockSize as usize]
                };

            let writtenSize = blockFile.write(data2Write)?;
            assert_eq!(writtenSize, data2Write.len());

            blockFile.sync_all()?;
        }

        Ok(())
    }

    fn scanDir(dbHeader: &DBHeader, dbOption: &DBOption) -> Result<(Vec<RawFd>, Vec<MemTable>)> {
        let mut blockFileFds = Vec::new();
        let mut immutableMemTables = Vec::new();

        for readDir in fs::read_dir(&dbOption.dirPath)? {
            let readDir = readDir?;

            let path = readDir.path();

            if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                match extension {
                    BLOCK_FILE_EXTENSION => { // block file
                        // block file size
                        if readDir.metadata()?.len() != dbHeader.blockSize as u64 {
                            throw!("block file size should be equal to blockSize in dbHeader");
                        }

                        let fileNum = utils::extractFileNum(&path).unwrap();
                        let blockFile = OpenOptions::new().read(true).write(true).open(path)?;

                        blockFileFds.push((fileNum, blockFile.as_raw_fd()));

                        mem::forget(blockFile);
                    }
                    MEM_TABLE_FILE_EXTENSION => { // memTable file 启动的时候已经存在的视为immutable的
                        let memTableFileLen = readDir.metadata()?.len() as usize;

                        let memTable = MemTable::open(&path, memTableFileLen)?;

                        // empty
                        if memTable.changes.is_empty() || memTable.header.written2Disk {
                            _ = memTable.destroy();
                            continue;
                        }

                        immutableMemTables.push(memTable);
                    }
                    _ => continue,
                }
            } else {
                continue;
            }
        }

        // sort by block num asc
        let blockFileFds = {
            blockFileFds.sort_by(|a, b| a.0.cmp(&b.0));
            blockFileFds.into_iter().map(|(_, fd)| fd).collect::<Vec<_>>()
        };

        // sort by memTable file num asc
        immutableMemTables.sort_by(|a, b| a.memTableFileNum.cmp(&b.memTableFileNum));

        Ok((blockFileFds, immutableMemTables))
    }

    fn processCommitReqs(db: Weak<Self>, commitReqReceiver: Receiver<CommitReq>) {
        // write changes in commitReq into memTable
        for commitReq in commitReqReceiver {
            match db.upgrade() {
                Some(db) => {
                    let mut memTable = db.memTable.write().unwrap();
                    memTable.processCommitReq(commitReq);
                }
                None => break,
            }
        }
    }

    fn processMemTableRs(db: Weak<Self>, memTableRReceiver: Receiver<MemTableR>, countThreshold: usize) {
        let mut vec: Vec<MemTableR> = Vec::with_capacity(countThreshold);

        // 收取了某些数量后再落地
        for memTableR in memTableRReceiver {
            match db.upgrade() {
                Some(db) => {
                    vec.push(memTableR);

                    if vec.len() >= countThreshold {
                        let batch = vec.drain(..).collect();
                        _ = mem_table_r::processMemTableRs(&*db, batch);
                    }
                }
                None => break,
            }
        }
    }

    fn generateBlockFile(dbOption: &DBOption, blockFileNum: usize, blockSize: usize) -> Result<File> {
        let blockFilePath = Path::join(
            dbOption.dirPath.as_ref(),
            format!("{}.{}", blockFileNum, BLOCK_FILE_EXTENSION),
        );

        let blockFile = OpenOptions::new().read(true).write(true).create_new(true).open(blockFilePath)?;

        blockFile.set_len(blockSize as u64)?;
        blockFile.sync_all()?;

        Ok(blockFile)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let blockFileFds = self.blockFileFds.read().unwrap();
        for dataFd in blockFileFds.iter() {
            let file = unsafe { File::from_raw_fd(*dataFd) };
            drop(file);
        }
    }
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub(crate) struct DBHeader {
    pub magic: usize,
    pub version: usize,
    /// 等同os的pageSize <br>
    /// linux 4KB, mac 16KB
    pub pageSize: usize,
    pub blockSize: usize,
    pub lastTxId: TxId,
    pub lastPageId: PageId,
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

        if self.pageSize > self.blockSize {
            throw!("page size should not exceed mmapUnitSize");
        }

        Ok(())
    }
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub(crate) struct BlockHeader;