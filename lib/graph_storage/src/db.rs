use std::fs::File;

const MAGIC: u32 = 0xCAFEBABE;
const VERSION: u32 = 1;

pub struct DB {
    pub dbFile: File,
    pub header: DBHeader,
}

pub struct DBHeader {
    pub magic: u32,
    pub version: u16,
    pub pageSize: u32,
}