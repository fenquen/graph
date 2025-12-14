use crate::mem_table::MemTable;
use crate::page::Page;
use crate::page_header::PageHeader;
use crate::tx::{CommitReq, Tx};
use crate::types::{PageId, TxId};
use crate::{page_header, utils};
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{mpsc, Arc, Mutex, RwLock, RwLockWriteGuard, Weak};
use std::{fs, mem, thread};
use std::mem::MaybeUninit;
use std::thread::JoinHandle;
use crate::lru_cache::LruCache;
use crate::mem_table_r::{MemTableR, MemTableRWriter};
use crate::page_allocator::{PageAllocatorWrapper};
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
pub(crate) const DEFAULT_PAGE_CACHE_SIZE: usize = 512;

pub(crate) const PAGE_CACHE_PARTITION_COUNT: usize = 64;

pub(crate) const BLOCK_FILE_EXTENSION: &str = "block";
pub(crate) const FIRST_BLOCK_FILE_NAME: &str = "0.block";
pub(crate) const MEM_TABLE_FILE_EXTENSION: &str = "mem";

pub(crate) const PAGE_ALLOCATOR_FILE_NAME: &str = "page_allocator";

/// 默认起始的时候最多有2^20个page,对应4GB容量
/// 分配page的时候分配的是连续的多个page,且分配的page数量是2的幂次方的
pub(crate) const INITIAL_PAGE_COUNT_ORDER: u8 = 20;

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

    pub pageCacheSize: usize,
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
            pageCacheSize: DEFAULT_PAGE_CACHE_SIZE,
        }
    }
}

pub struct DB {
    pub(crate) dbOption: DBOption,

    headerMmapMut: MmapMut,
    header: &'static mut DBHeader,

    lock: Mutex<()>,
    // txIdCounter: AtomicU64,
    // pageIdCounter: AtomicU64,

    /// sorted by block file number
    blockFileNum2Fd: RwLock<HashMap<usize, RawFd>>,

    pageCaches: Vec<RwLock<LruCache>>,

    pub(crate) memTable: RwLock<MemTable>,
    pub(crate) immutableMemTables: RwLock<Vec<MemTable>>,

    pub(crate) pageAllocator: RwLock<PageAllocatorWrapper>,

    pub(crate) commitReqSender: SyncSender<CommitReq>,
    pub(crate) memTableRSender: SyncSender<MemTableR>,

    pub(crate) joinHandleCommitReqs: MaybeUninit<JoinHandle<()>>,
    pub(crate) joinHandleMemTableRs: MaybeUninit<JoinHandle<()>>,

    pub(crate) flyingTxIds: RwLock<BTreeSet<TxId>>,

    /// 当page因为要容纳的内容太多而分裂为多个后,分裂后的各个新page最多可以
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

        /* if shouldInit { // init时候会生成pageId是0和1的两个page的
             //  dbHeader.lastPageId = 1;
         }*/

        // memTableMaxSize要是实际的pageSize整数
        dbOption.memTableMaxSize = utils::roundUp2Multiple(dbOption.memTableMaxSize, dbHeader.pageSize);

        // 启动的时候将已经存在的memTable文件都视为immutable的
        let (blockFileNum2Fd, immutableMemTables) = DB::scanDir(dbHeader, &dbOption)?;

        // 当启动的时候总是会新生成1个作为当前的mutableMemTable
        let memTable = {
            let mutableMemTableFileNum =
                immutableMemTables.last().map_or_else(|| 0, |memTable| memTable.memTableFileNum + 1);

            let mutableMemTableFilePath =
                Path::join(dbOption.dirPath.as_ref(), format!("{}.{}", mutableMemTableFileNum, MEM_TABLE_FILE_EXTENSION));

            MemTable::open(mutableMemTableFilePath, dbOption.memTableMaxSize)?
        };

        // pageAllocator
        let pageAllocator =
            PageAllocatorWrapper::open(Path::join(dbOption.dirPath.as_ref(), PAGE_ALLOCATOR_FILE_NAME),
                                       INITIAL_PAGE_COUNT_ORDER)?;

        let (commitReqSender, commitReqReceiver) =
            mpsc::sync_channel::<CommitReq>(dbOption.commitReqChanBufSize);

        let (memTableRSender, memTableRReceiver) =
            mpsc::sync_channel::<MemTableR>(dbOption.memTableRChanBufSize);

        let pageCaches = (0..PAGE_CACHE_PARTITION_COUNT).map(|_| RwLock::new(LruCache::new(dbOption.pageCacheSize))).collect::<Vec<_>>();

        let availablePageSizeAfterSplit = f64::ceil(dbHeader.pageSize as f64 * dbOption.pageFillPercentAfterSplit) as usize;

        let db =
            Arc::new(
                DB {
                    dbOption,
                    header: dbHeader,
                    headerMmapMut: dbHeaderMmap,
                    lock: Mutex::new(()),
                    // 接着原来的保存在dbHeader的txId
                    //txIdCounter: AtomicU64::new(dbHeader.lastTxId + 1),
                    // pageIdCounter: AtomicU64::new(dbHeader.lastPageId + 1),
                    blockFileNum2Fd: RwLock::new(blockFileNum2Fd),
                    pageCaches,
                    memTable: RwLock::new(memTable),
                    immutableMemTables: RwLock::new(immutableMemTables),
                    pageAllocator: RwLock::new(pageAllocator),
                    commitReqSender,
                    memTableRSender,
                    joinHandleCommitReqs: MaybeUninit::<JoinHandle<()>>::uninit(),
                    joinHandleMemTableRs: MaybeUninit::<JoinHandle<()>>::uninit(),
                    flyingTxIds: Default::default(),
                    availablePageSizeAfterSplit,
                }
            );

        if shouldInit {
            // page0 是用来保存dbheader的 不对外使用 需要保留
            {
                let mut pageAllocator = db.pageAllocator.write().unwrap();
                pageAllocator.allocate(db.header.pageSize, db.header.pageSize);
            }

            // page1 是默认起始的rootPage 且当时是leaf的
            db.allocatePagesByCount(1, db.header.pageSize, page_header::PAGE_FLAG_LEAF)?;
        }

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

            {
                let mut pageAllocator = db.pageAllocator.write().unwrap();
                pageAllocator.db = Arc::downgrade(&db);
            }
        }

        // 事务提交处理的thread
        let dbClone = Arc::downgrade(&db);
        let joinHandleCommitReqs =
            thread::Builder::new().name("process_commit_reqs".to_string()).spawn(move || {
                DB::processCommitReqs(dbClone, commitReqReceiver);
            })?;

        // 落地immutableMemTable的thread
        let weakDb = Arc::downgrade(&db);
        let immutableMemTableCount = db.dbOption.immutableMemTableCount;
        let joinHandleMemTableRs =
            thread::Builder::new().name("process_mem_table_rs".to_string()).spawn(move || {
                DB::processMemTableRs(weakDb, memTableRReceiver, immutableMemTableCount);
            })?;

        // 使用非正常的手段设置
        {
            let db = unsafe { &mut *{ Arc::as_ptr(&db) as *mut DB } };
            db.joinHandleCommitReqs.write(joinHandleCommitReqs);
            db.joinHandleMemTableRs.write(joinHandleMemTableRs);
        }

        //  启动的时候这些遗留的immutableMemTables需要发送到对应的处理线程去落地
        {
            let immutableMemTables = db.immutableMemTables.read().unwrap();

            for memTable in immutableMemTables.iter() {
                let memTableR = MemTableR::try_from(memTable.memTableFileFd)?;
                db.memTableRSender.send(memTableR)?;
            }
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
        self.header
    }

    #[inline]
    pub(crate) fn getHeaderMut(&self) -> &mut DBHeader {
        unsafe {
            mem::transmute(self.header as *const _ as *mut DBHeader)
        }
    }

    /// 尝试从lru获取,要是没有就读取blockFile然后mmap映射,并将其加入lru
    /// page得是有效的(header.flags != PAGE_FLAG_INVALID)
    pub(crate) fn getPageById(&self, pageId: PageId) -> Result<Arc<RwLock<Page>>> {
        let dbHeader = self.getHeader();

        let mut pageCache = self.locateLru(pageId).write().unwrap();

        if let Some(page) = pageCache.get(pageId) {
            return Ok(page.clone());
        }

        // todo 需要java的获取单例的double check套路
        let targetBlockFileFd = {
            let blockFileFds = self.blockFileNum2Fd.read().unwrap();
            let blockFileNum = self.blockFileNum(pageId);
            blockFileFds.get(&blockFileNum).unwrap().clone()
        };

        let pageMmapMut = {
            let pageHeaderOffsetInBlock = (dbHeader.pageSize as u64 * pageId) % dbHeader.blockSize as u64;
            utils::mmapFdMut(targetBlockFileFd, Some(pageHeaderOffsetInBlock), Some(dbHeader.pageSize))?
        };

        let page = Page::restore(self, pageMmapMut)?;

        // 对应的page得是已经创建过的了(调用过了buildPageById)不是空白的
        if page.header.flags == page_header::PAGE_FLAG_INVALID {
            throw!(format!("page [id:{}] is not valid",pageId));
        }

        let page = Arc::new(RwLock::new(page));
        pageCache.set(pageId, page.clone());

        Ok(page)
    }

    #[inline]
    fn locateLru(&self, pageId: PageId) -> &RwLock<LruCache> {
        &self.pageCaches[pageId as usize % PAGE_CACHE_PARTITION_COUNT]
    }

    fn blockFileNum(&self, pageId: PageId) -> usize {
        let dbHeader = self.getHeader();
        (dbHeader.pageSize * pageId as usize) / dbHeader.blockSize
    }

    /// txId increase
    pub(crate) fn allocateTxId(&self) -> Result<TxId> {
        let dbHeader = self.getHeaderMut();

        let txId = {
            let lock = self.lock.lock().unwrap();
            let txId = dbHeader.lastTxId;
            dbHeader.lastTxId += 1;
            self.headerMmapMut.flush()?;
            txId
        };

        Ok(txId)
    }

    /*#[deprecated]
    pub(crate) fn allocateNewPage(&self, flags: u16) -> Result<Page> {
        let pageId = self.allocatePageId()?;
        self.buildPageById(pageId, flags)
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
    }*/

    fn buildPageById(&self, pageId: PageId, flags: u16) -> Result<Page> {
        // 需要blockSize 和 pageSize
        let dbHeader = self.getHeader();

        // 得到这个page应该是在哪个blockFile
        // blockFile可能需新建也能依然有了
        let blockFileNum = self.blockFileNum(pageId);

        let mut blockFileNum2Fd = self.blockFileNum2Fd.write().unwrap();

        // blockFile尚未创建
        if blockFileNum2Fd.contains_key(&blockFileNum) == false {
            let blockFile = DB::generateBlockFile(&self.dbOption, blockFileNum, self.getHeader().blockSize)?;
            blockFileNum2Fd.insert(blockFileNum, blockFile.as_raw_fd());
            mem::forget(blockFile);
        }

        let blockFileFds = RwLockWriteGuard::downgrade(blockFileNum2Fd);
        let targetBlockFileFd =
            match blockFileFds.get(&blockFileNum) {
                Some(blockFileFd) => *blockFileFd,
                None => panic!("{}", format!("blockFile num:[{}] not exist should not happen", blockFileNum)),
            };

        let mut pageMmap = {
            let pageHeaderOffsetInBlock = (dbHeader.pageSize as u64 * pageId) % dbHeader.blockSize as u64;
            utils::mmapFdMut(targetBlockFileFd, Some(pageHeaderOffsetInBlock), Some(dbHeader.pageSize))?
        };

        let pageHeader = {
            let pageHeader: &mut PageHeader = utils::slice2RefMut(&mut pageMmap);

            /*if pageHeader.flags != page_header::PAGE_FLAG_INVALID {
                throw!(format!("page [id:{}] is not valid",pageId));
            }*/

            pageHeader.id = pageId;
            pageHeader.flags = flags;

            pageHeader
        };

        Ok(Page {
            db: unsafe { mem::transmute(self) },
            //parentPage: None,
            // indexInParentPage: None,
            mmapMut: pageMmap,
            header: pageHeader,
            pageElems: vec![],
            //keyMin: Default::default(),
            //keyMax: Default::default(),
            //childPages: None,
            additionalPages: vec![],
            replacement: None,
        })
    }

    #[inline]
    pub(crate) fn allocatePagesByCount(&self,
                                       expectCount: usize, pageSize: usize,
                                       flags: u16) -> Result<Vec<Page>> {
        self.allocatePagesBySize(pageSize * expectCount, pageSize, flags)
    }

    /// 要传入pageSize的原因是 如果leafPage会分裂的话,新分裂生成的各个leafPage不能使用全部的pageSize的
    pub(crate) fn allocatePagesBySize(&self,
                                      expectSize: usize, pageSize: usize,
                                      flags: u16) -> Result<Vec<Page>> {
        let mut pageAllocator = self.pageAllocator.write().unwrap();

        let (mut pageId, pageCount) = pageAllocator.allocate(expectSize, pageSize).unwrap();

        let mut pages = Vec::with_capacity(pageCount);

        for _ in 0..pageCount {
            pages.push(self.buildPageById(pageId, flags)?);
            pageId += 1;
        }

        Ok(pages)
    }

    /// 对page的free涉及: 数据文件对应的,allocator,lrucache
    pub(crate) fn free(&self, page: &mut Page) {
        page.invalidate();

        let mut pageAllocator = self.pageAllocator.write().unwrap();
        pageAllocator.free(page.header.id, 1);

        // 直接从lru给干掉
        let mut pageCache = self.locateLru(page.header.id).write().unwrap();
        pageCache.remove(page.header.id);
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

    // page0用来保存dbHeader,page1是起始的用来保存data的,它们都保存在blockFile0
    fn init(dbOption: &DBOption) -> Result<()> {
        // 确保用户自定义的pageSize是os的pageSize整数
        let pageSize = utils::roundUp2Multiple(dbOption.pageSize, *utils::OS_PAGE_SIZE);

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
        let page1Header: &mut PageHeader = utils::slice2RefMut(&mut first2PageSpace[pageSize..]);
        page1Header.id = 1;
        page1Header.flags = page_header::PAGE_FLAG_LEAF;

        // idea from bbolt,用来计算占用的数量而不是对应的block的index的
        let blockCount = ((pageSize * 2) + dbHeader.blockSize - 1) / dbHeader.blockSize;

        // 生成各个需要的block对应文件
        for blockFileNum in 0..blockCount {
            let mut blockFile = DB::generateBlockFile(dbOption, blockFileNum, dbHeader.blockSize)?;

            // extend to blockSize
            blockFile.set_len(dbHeader.blockSize as u64)?;

            let data2Write =
                // last part
                if blockFileNum == blockCount - 1 {
                    &first2PageSpace[blockFileNum * dbHeader.blockSize..]
                } else {
                    &first2PageSpace[blockFileNum * dbHeader.blockSize..(blockFileNum + 1) * dbHeader.blockSize]
                };

            let writtenSize = blockFile.write(data2Write)?;
            assert_eq!(writtenSize, data2Write.len());

            blockFile.sync_all()?;
        }

        Ok(())
    }

    /// 扫描data目录,对已存在的mem文件需要还原
    fn scanDir(dbHeader: &DBHeader, dbOption: &DBOption) -> Result<(HashMap<usize, RawFd>, Vec<MemTable>)> {
        let mut blockFileNum2Fd = HashMap::new();
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

                        blockFileNum2Fd.insert(fileNum, blockFile.as_raw_fd());

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

        // sort by memTable file num asc
        immutableMemTables.sort_by(|a, b| a.memTableFileNum.cmp(&b.memTableFileNum));

        Ok((blockFileNum2Fd, immutableMemTables))
    }

    fn processCommitReqs(db: Weak<DB>, commitReqReceiver: Receiver<CommitReq>) {
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

    fn processMemTableRs(db: Weak<DB>, memTableRReceiver: Receiver<MemTableR>, countThreshold: usize) {
        let mut vec: Vec<MemTableR> = Vec::with_capacity(countThreshold);
        let mut memTableRWriter = MemTableRWriter::new();

        // 收取了某些数量后再落地
        for memTableR in memTableRReceiver {
            match db.upgrade() {
                Some(db) => {
                    vec.push(memTableR);

                    if vec.len() >= countThreshold {
                        let batch = vec.drain(..).collect();
                        _ = memTableRWriter.processMemTableRs(&*db, batch);
                    }
                }
                None => break,
            }
        }
    }

    fn generateBlockFile(dbOption: &DBOption, blockFileNum: usize, blockSize: usize) -> Result<File> {
        let blockFilePath =
            Path::join(dbOption.dirPath.as_ref(),
                       format!("{}.{}", blockFileNum, BLOCK_FILE_EXTENSION));

        let blockFile = OpenOptions::new().read(true).write(true).create_new(true).open(blockFilePath)?;

        blockFile.set_len(blockSize as u64)?;
        blockFile.sync_all()?;

        Ok(blockFile)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let blockFileFds = self.blockFileNum2Fd.read().unwrap();
        for (_, dataFd) in blockFileFds.iter() {
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
    // pub lastPageId: PageId,
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