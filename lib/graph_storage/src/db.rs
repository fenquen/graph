use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use anyhow::{bail, Result};
use lazy_static::lazy_static;
use crate::utils;

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;
const DB_HEADER_LEN: usize = 100;

pub struct DB {
    pub file: File,
    pub header: DBHeader,
}

impl DB {
    pub fn open(path: &str) -> Result<DB> {
        let dbFile = OpenOptions::new().read(true).write(true).create(true).open(path)?;

        let mut db = DB {
            file: dbFile,
            header: Default::default(),
        };

        // open the alreay exsit db file
        if db.file.metadata()?.len() > 0 {
            db.validate()?
        } else {
            db.init()?;
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
            bail!("incorrect version");
        }

        // the db file is from another os 
        // use custom page size
        if self.header.pageSize != utils::getOsPageSize() {
            if utils::isPowerOfTwo(self.header.pageSize) == false {
                throw!("incorrect page size");
            }
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        let mut pageBuf = [0u8; DB_HEADER_LEN];

        unsafe {
            let dbHeader = pageBuf.as_ptr() as *mut DBHeader;
            (*dbHeader).magic = MAGIC;
            (*dbHeader).version = VERSION;
            (*dbHeader).pageSize = utils::getOsPageSize();

            self.header = *dbHeader;
        }

        self.file.write_at(pageBuf.as_slice(), 0)?;

        Ok(())
    }
}

#[derive(Default, Clone, Copy)]
#[repr(C)]
pub struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: u16,
}