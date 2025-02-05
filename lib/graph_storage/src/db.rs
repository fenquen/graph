use crate::utils;
use anyhow::{Result};
use std::fmt::Display;
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;
const DB_HEADER_LEN: usize = 100;
/// 64MB
const DEFAULT_MMAP_UNIT_LEN: u32 = 1024 * 1024 * 64;

pub struct DB {
    pub dirPath: String,
    pub file: File,
    pub header: DBHeader,
}

impl DB {
    pub fn open(dbOption: &DBOption) -> Result<DB> {
        Self::verifyDirPath(&dbOption.dirPath)?;

        let dbFile = OpenOptions::new().read(true).write(true).create(true).open(&dbOption.dirPath)?;

        let mut db = DB {
            file: dbFile,
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
            // open the already exsit db file
            db.validate()?;
        }

        Ok(db)
    }

    fn validate(&mut self) -> Result<()> {
        let mut pageBuf = [0u8; DB_HEADER_LEN];
        let readLen = self.file.read_at(pageBuf.as_mut_slice(), 0)?;
        if readLen != DB_HEADER_LEN {
            throw!("incorrect length");
        }

        unsafe {
            self.header = *(pageBuf.as_ptr() as *const DBHeader);
        }

        if self.header.magic != MAGIC {
            throw!("incorrect magic");
        }

        if self.header.version != VERSION {
            throw!("incorrect version");
        }

        // the db file may from another os,page size is different
        if utils::isPowerOfTwo(self.header.pageSize) == false {
            throw!("page size must be a power of two");
        }

        if self.header.mmapUnitSize != self.file.metadata()?.len() as u32 {
            throw!("incorrect mmap size");
        }

        if utils::isPowerOfTwo(self.header.mmapUnitSize) == false {
            throw!("mmap unit size must be a power of two");
        }

        //   mmapUnitSize % single db file size should be 0

        Ok(())
    }

    fn init(&mut self, dbOption: &DBOption) -> Result<()> {
        let mut pageBuf = [0u8; DB_HEADER_LEN];

        unsafe {
            let dbHeader = pageBuf.as_ptr() as *mut DBHeader;
            (*dbHeader).magic = MAGIC;
            (*dbHeader).version = VERSION;
            (*dbHeader).pageSize = utils::getOsPageSize();
            (*dbHeader).mmapUnitSize = if dbOption.mmapUnitSize > 0 {
                dbOption.mmapUnitSize
            } else {
                DEFAULT_MMAP_UNIT_LEN
            };

            self.header = *dbHeader;

            // extend as big as mmapUnitSize
            self.file.set_len((*dbHeader).mmapUnitSize as u64)?
        }

        self.file.sync_all()?;

        self.file.write_at(pageBuf.as_slice(), 0)?;

        Ok(())
    }

    fn verifyDirPath(dirPath: impl AsRef<Path> + Display) -> Result<()> {
        if Path::exists(&dirPath) == false {
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

pub struct DBOption {
    pub dirPath: String,
    pub mmapUnitSize: u32,
}