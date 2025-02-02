use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use anyhow::Result;
use crate::utils;

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u16 = 1;

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
        let mut pageBuf = [0u8; 4096];
        self.file.read_at(pageBuf.as_mut_slice(), 0)?;

        unsafe {
            let a = pageBuf.as_ptr() as *const DBHeader;
            println!("{}", (*a).magic);
            println!("{}", (*a).version);
            println!("{}", (*a).pageSize);
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        let mut pageBuf = [0u8; 4096];

        unsafe {
            let dbHeader = pageBuf.as_ptr() as *mut DBHeader;
            (*dbHeader).magic = MAGIC;
            (*dbHeader).version = VERSION;
            (*dbHeader).pageSize = utils::getPageSize();
        }

        self.file.write_at(pageBuf.as_slice(), 0)?;

        Ok(())
    }
}

#[derive(Default)]
//#[repr(C)]
pub struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: usize,
}