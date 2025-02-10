use crate::utils;
use anyhow::{Result};
use std::fmt::Display;
use std::{fs, mem};
use std::fs::{File, OpenOptions};
use std::mem::{forget, ManuallyDrop};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use libc::mmap;
use memmap2::{Advice, MmapMut, MmapOptions};

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;

pub(crate) const DB_HEADER_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 * 1;

pub struct DB {
    data0Fd: RawFd,
    dbHeaderMmap: MmapMut,
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

        let data0 =
            if shouldInit {
                // AsRef::<Path>::as_ref(&self.dirPath).join("data_0");
                let data0Path = Path::join(dbOption.dirPath.as_ref(), "0.data");
                let data0 = OpenOptions::new().read(true).write(true).create_new(true).open(data0Path)?;

                // extend as big as mmapUnitSize
                let blockSize = if dbOption.blockSize > 0 {
                    dbOption.blockSize
                } else {
                    DEFAULT_BLOCK_SIZE
                };
                data0.set_len(blockSize as u64)?;
                data0.sync_all()?;

                data0
            } else { // open the already exsit db files
                let data0Path = Path::join(dbOption.dirPath.as_ref(), "0.data");
                let data0 = OpenOptions::new().read(true).write(true).open(data0Path)?;

                if data0.metadata()?.len() < DB_HEADER_SIZE as u64 {
                    throw!("0.data size should be greate than db DB_HEADER_SIZE");
                }

                data0
            };

        let mut dbHeaderMmap = unsafe {
            let mmapMut = {
                let mut mmapOptions = MmapOptions::new();
                mmapOptions.offset(0).len(DB_HEADER_SIZE);
                mmapOptions.map_mut(data0.as_raw_fd())?
            };

            mmapMut.advise(Advice::WillNeed)?;
            mmapMut.lock()?;

            mmapMut
        };

        let dbHeader = (&mut dbHeaderMmap[..]).as_ptr() as *mut DBHeader;
        let dbHeader = unsafe { mem::transmute::<*const DBHeader, &mut DBHeader>(dbHeader) };

        if shouldInit {
            dbHeader.magic = MAGIC;
            dbHeader.version = VERSION;
            dbHeader.pageSize = utils::getOsPageSize();
            dbHeader.blockSize = if dbOption.blockSize > 0 {
                dbOption.blockSize
            } else {
                DEFAULT_BLOCK_SIZE
            };

            dbHeaderMmap.flush()?;
        }

        dbHeader.validate()?;

        for readDir in fs::read_dir(&dbOption.dirPath)? {
            let readDir = readDir?;

            if readDir.metadata()?.len() != dbHeader.blockSize as u64 {
                throw!("data file size should be equal to block size");
            }
        }

        let db = DB {
            data0Fd: data0.as_raw_fd(),
            dbHeaderMmap,
        };

        let _ = ManuallyDrop::new(data0);

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
    
    
    
}

impl Drop for DB {
    fn drop(&mut self) {
        if self.data0Fd != 0 {
            let file = unsafe { File::from_raw_fd(self.data0Fd) };
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