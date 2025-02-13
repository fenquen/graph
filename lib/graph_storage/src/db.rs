use crate::{page_header, utils};
use anyhow::{Result};
use std::fmt::Display;
use std::{fs, mem};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{forget, ManuallyDrop};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use dashmap::mapref::one::RefMut;
use libc::read;
use memmap2::{Advice, Mmap, MmapMut, MmapOptions};
use crate::page::Page;
use crate::page_header::PageHeader;
use crate::types::{PageId, TxId};

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;

pub(crate) const DB_HEADER_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 * 1;

pub struct DB {
    dbHeaderMmap: MmapMut,
    /// source from dbHeaderMap
    dbHeader: &'static DBHeader,
    /// sorted by block file number
    dataFds: Vec<RawFd>,
    pageId2Page: DashMap<PageId, Arc<RwLock<Page>>>,
    memTable: HashMap<Vec<u8>, Vec<u8>>,
    immutableMemTables: Vec<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DB {
    pub fn open(dbOption: &DBOption) -> Result<DB> {
        Self::verifyDirPath(&dbOption.dirPath)?;

        let shouldInit = {
            let mut shouldInit = true;

            for dirEntry in fs::read_dir(&dbOption.dirPath)? {
                _ = dirEntry?;
                shouldInit = false;
                break;
            }

            shouldInit
        };

        let block0FileFd =
            // generate dbHeader and initial some pages, write them into files
            if shouldInit {
                // allocate space for 2 pages in memory
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
                dbHeader.nextTxId = 0;
                dbHeader.rootPageId = 1;

                // transmute as pageHeader in page0
                let page0Header: &mut PageHeader = unsafe { utils::slice2RefMut(&mut pageSpace[DB_HEADER_SIZE..]) };
                page0Header.pageId = 0;
                page0Header.flags = page_header::PAGE_FLAG_META;

                // transmute as pageHeader in page1
                let page0Header: &mut PageHeader = unsafe { utils::slice2RefMut(&mut pageSpace[pageSize as usize..]) };
                page0Header.pageId = 1;
                page0Header.flags = page_header::PAGE_FLAG_LEAF;

                // bbolt
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

                block0Fd
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
        let mut dbHeaderMmap = unsafe {
            let mmapMut = {
                let mut mmapOptions = MmapOptions::new();
                mmapOptions.offset(0).len(DB_HEADER_SIZE);
                mmapOptions.map_mut(block0FileFd)?
            };

            mmapMut.advise(Advice::WillNeed)?;
            mmapMut.lock()?;

            mmapMut
        };

        let dbHeader = unsafe { utils::slice2RefMut::<'static, DBHeader>(&mut dbHeaderMmap) };
        dbHeader.validate()?;

        // try to hold all data file fds
        let dataFds = {
            let mut dataFds = Vec::new();

            dataFds.push((0, block0FileFd));

            for readDir in fs::read_dir(&dbOption.dirPath)? {
                let readDir = readDir?;

                let path = readDir.path();

                if !path.ends_with(".data") {
                    continue;
                }

                let dataFileNameStr = path.file_name().unwrap().to_str().unwrap();
                let elemVec = dataFileNameStr.split(".").collect::<Vec<&str>>();
                if 1 >= elemVec.len() {
                    continue;
                }

                let blockNum = u64::from_str(elemVec.get(0).unwrap())?;

                // block file size
                if readDir.metadata()?.len() != dbHeader.blockSize as u64 {
                    throw!("block file size should be equal to blockSize in dbHeader");
                }

                // already added
                if blockNum == 0 {
                    continue;
                }

                let dataFile = OpenOptions::new().read(true).write(true).open(path)?;
                dataFds.push((blockNum, dataFile.as_raw_fd()));
                let _ = ManuallyDrop::new(dataFile);
            }

            dataFds.sort_by(|a, b| { a.0.cmp(&b.0) });

            dataFds.into_iter().map(|x| x.1).collect::<Vec<_>>()
        };

        let db = DB {
            dbHeaderMmap,
            dbHeader,
            dataFds,
            pageId2Page: DashMap::new(),
            memTable: Default::default(),
            immutableMemTables: Default::default(),
        };

        Ok(db)
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

    pub(crate) fn getPageById(&self,
                              pageId: PageId,
                              parentPage: Option<Arc<RwLock<Page>>>) -> Result<Arc<RwLock<Page>>> {
        if let Some(page) = self.pageId2Page.get(&pageId) {
            return Ok(page.clone());
        }

        let targetBlockFd = {
            let blockNum = (self.dbHeader.pageSize as u64 * pageId) / self.dbHeader.blockSize as u64;
            self.dataFds.get(blockNum as usize).unwrap()
        };

        let pageMmap = {
            let pageHeaderOffsetInBlock = (self.dbHeader.pageSize as u64 * pageId) % self.dbHeader.blockSize as u64;
            utils::mmapFd(*targetBlockFd, pageHeaderOffsetInBlock, self.dbHeader.pageSize as usize)?
        };

        let pageHeader = unsafe { utils::slice2Ref::<'static, PageHeader>(&pageMmap) };

        let mut page = Page::readFromPageHeader(pageMmap, pageHeader);
        page.parentPage = parentPage;

        let page = Arc::new(RwLock::new(page));
        self.pageId2Page.insert(pageId, page.clone());

        Ok(page)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        for dataFd in self.dataFds.iter() {
            let file = unsafe { File::from_raw_fd(*dataFd) };
            drop(file);
        }
    }
}

pub struct DBOption {
    pub dirPath: String,
    pub blockSize: u32,
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub(crate) struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: u16,
    pub blockSize: u32,
    pub nextTxId: TxId,
    pub rootPageId: PageId,
}

impl DBHeader {
    pub fn validate(&self) -> Result<()> {
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