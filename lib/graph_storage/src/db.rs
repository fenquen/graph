use crate::utils;
use anyhow::{Result};
use std::fmt::Display;
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use memmap2::MmapMut;

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;
const DB_HEADER_LEN: usize = 100;
const DEFAULT_MMAP_UNIT_LEN: u32 = 1024 * 1024 * 1;


pub struct DB {
    pub dirPath: String,
    pub header: DBHeader,
}

impl DB {
    pub fn open(dbOption: &DBOption) -> Result<DB> {
        Self::verifyDirPath(&dbOption.dirPath)?;
       
        let mut db = DB {
            header: Default::default(),
            dirPath: dbOption.dirPath.clone(),
        };

        let shouldInit = {
            let mut shouldInit = true;

            for dirEntry in fs::read_dir(&dbOption.dirPath)? {
                _ = dirEntry?;
                shouldInit = false;
                break;
            }

            shouldInit
        };

        if shouldInit {
            db.init(dbOption)?;
        } else {
            // open the already exsit db files
            db.validate()?;
        }

        Ok(db)
    }

    fn validate(&mut self) -> Result<()> {
        let mut dbHeaderBuf = [0u8; DB_HEADER_LEN];

        let data0Path = Path::join(self.dirPath.as_ref(), "0.data");
        let data0 = OpenOptions::new().read(true).write(true).open(data0Path)?;

        let readLen = data0.read_at(dbHeaderBuf.as_mut_slice(), 0)?;
        if readLen != DB_HEADER_LEN {
            throw!("incorrect length");
        }

        unsafe {
            self.header = *(dbHeaderBuf.as_ptr() as *const DBHeader);
        }

        self.header.validate()?;

        // mmapUnitSize should equal with file length
        if self.header.mmapUnitSize != data0.metadata()?.len() as u32 {
            throw!("incorrect mmap size");
        }

        Ok(())
    }

    fn init(&mut self, dbOption: &DBOption) -> Result<()> {
        let dbHeaderBuf = [0u8; DB_HEADER_LEN];

        unsafe {
            let dbHeader = dbHeaderBuf.as_ptr() as *mut DBHeader;
            (*dbHeader).magic = MAGIC;
            (*dbHeader).version = VERSION;
            (*dbHeader).pageSize = utils::getOsPageSize();
            (*dbHeader).mmapUnitSize = if dbOption.mmapUnitSize > 0 {
                dbOption.mmapUnitSize
            } else {
                DEFAULT_MMAP_UNIT_LEN
            };

            self.header = *dbHeader;

            self.header.validate()?;
        }

        // AsRef::<Path>::as_ref(&self.dirPath).join("data_0");
        let data0Path = Path::join(self.dirPath.as_ref(), "0.data");
        let data0 = OpenOptions::new().read(true).write(true).create_new(true).open(data0Path)?;

        // extend as big as mmapUnitSize
        data0.set_len(self.header.mmapUnitSize as u64)?;

        data0.write_at(dbHeaderBuf.as_slice(), 0)?;

        data0.sync_all()?;

        Ok(())
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

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: u16,
    pub mmapUnitSize: u32,
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

        if utils::isPowerOfTwo(self.mmapUnitSize) == false {
            throw!("mmap unit size must be a power of two");
        }

        if self.pageSize as u32 > self.mmapUnitSize {
            throw!("page size should not exceed mmapUnitSize");
        }

        Ok(())
    }
}

pub struct DBOption {
    pub dirPath: String,
    pub mmapUnitSize: u32,
}