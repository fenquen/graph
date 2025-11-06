use crate::tx::CommitReq;
use crate::{tx, utils};
use anyhow::Result;
use memmap2::{Advice, MmapMut};
use std::collections::{BTreeMap};
use std::{fs, mem};
use std::fs::{File, OpenOptions};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use crate::db::{DB, MEM_TABLE_FILE_EXTENSION};
use crate::mem_table_r::MemTableR;

/// memTable 文件结构
/// | entryCount | keySize | valSize | 实际data | keySize | valSize | 实际data |
pub(crate) struct MemTable {
    pub(crate) memTableFileNum: usize,

    /// 因为通过file struct 得不到对应的path,只能这里单独记录了
    memTableFilePath: PathBuf,

    /// underlying data file fd
    pub(crate) memTableFileFd: RawFd,

    memTableFileMmap: MmapMut,

    /// 不变
    maxFileSizeBeforeSwitch: usize,

    posInFile: usize,

    /// 会随着1个新的tx清零
    writtenEntryCountInTx: usize,

    //#[cfg(target_os = "macos")]
    currentFileSize: usize,

    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,

    /// db本身会持有MemTable,需要使用weak的
    pub(crate) db: Weak<DB>,

    pub(crate) immutable: bool,

    pub(crate) header: &'static mut MemTableFileHeader,
}

impl MemTable {
    /// restore existed memTable file / create a new one
    pub(crate) fn open(memTableFilePath: impl AsRef<Path>, newMemTableFileSize: usize) -> Result<MemTable> {
        let memTableFileNum = utils::extractFileNum(&memTableFilePath).unwrap();

        let alreadyExisted = fs::exists(&memTableFilePath)?;

        let memTableFile = {
            let mut openOptions = OpenOptions::new();
            openOptions.read(true).write(true).create(true);

            openOptions.open(&memTableFilePath)?
        };

        if alreadyExisted == false {
            memTableFile.set_len(newMemTableFileSize as u64)?;

            // 整个函数的消耗约4ms,单单它的耗时占到99%以上的
            // 而且1直在纠结到底有没有必要使用sync_all()
            // memTableFile.sync_all()?;
        }

        let memTableFileFd = {
            let memTableFileFd = memTableFile.as_raw_fd();
            mem::forget(memTableFile);
            memTableFileFd
        };

        // 文件如果是writable的且mmap不是私有映射,mmap超出了文件会扩展文件
        // 这样的话应该就不用上边的sync_all(),因为后续写完tx的changes还是会调用msync()的
        //
        // mmap2有个很坑的地方,要map的字节的长度虽然类型是usize不能超过isize::MAX,详见其validate_len()
        let memTableFileMmap = utils::mmapFdMut(memTableFileFd, None, Some(1024 * 1024 * 1024))?;

        // macos特殊点
        // macOS 对 MADV_WILLNEED 有严格限制,要求操作的内存范围必须对应文件中已存在的数据块（即不超过文件当前大小）
        // 若范围包含超出文件大小的部分（即使 mmap 预留了更大的虚拟地址),操作系统会认为该请求无效返回EINVAL(Invalid argument)
        //
        // Linux 对 MADV_WILLNEED 更宽松，允许对超出文件大小的映射区域调用(但预读无效仅忽略超出部分),而macos直接报错的
        #[cfg(target_os = "macos")]
        memTableFileMmap.advise_range(Advice::WillNeed, 0, newMemTableFileSize)?;
        #[cfg(target_os = "linux")]
        memTableFileMmap.advise(Advice::WillNeed)?;

        let header = utils::slice2RefMut(&memTableFileMmap);

        let mut memTable = MemTable {
            memTableFileNum,
            memTableFilePath: memTableFilePath.as_ref().to_owned(),
            memTableFileFd,
            memTableFileMmap,
            maxFileSizeBeforeSwitch: newMemTableFileSize,
            posInFile: MEM_TABLE_FILE_HEADER_SIZE,
            writtenEntryCountInTx: 0,
            //#[cfg(target_os = "macos")]
            currentFileSize: newMemTableFileSize,
            changes: BTreeMap::new(),
            db: Weak::new(),
            // if the file already exist, then it is immutable
            immutable: alreadyExisted,
            header,
        };

        // 说明是新创建的,不用replay
        if alreadyExisted == false {
            return Ok(memTable);
        }

        // replay
        memTable.replay()?;

        Ok(memTable)
    }

    /// 回放memTable,memTable其实也起到了wal用途
    pub(crate) fn replay(&mut self) -> Result<()> {
        assert!(self.immutable);

        for _ in 0..self.header.entryCount as usize {
            let (key, value, entrySize) = {
                let (key, value, entrySize) =
                    readEntry(&self.memTableFileMmap[self.posInFile..]);

                (
                    key.to_vec(),
                    value.map(|value| value.to_vec()),
                    entrySize
                )
            };

            self.changes.insert(key, value);
            self.posInFile += entrySize;
        }

        Ok(())
    }

    /// protected by write mutex
    pub(crate) fn processCommitReq(&mut self, commitReq: CommitReq) {
        assert_eq!(self.immutable, false);

        let process = || {
            // 原来的posInFile
            let _ = self.posInFile;

            for (keyWithoutTxId, value) in commitReq.changes {
                let keyWithTxId = tx::appendKeyWithTxId0(keyWithoutTxId, commitReq.txId);
                self.writeChange(keyWithTxId, value)?;
            }

            // 说明当前的这个memTable已经写满了,要换新的了
            // switch2NewMemTable现在是整个事务级别的触发
            // 原来是事务中的单个kv的写入来触发,这是不对的因为事务是个整体,不能细化到其中的某kv
            if self.posInFile >= self.maxFileSizeBeforeSwitch {
                self.switch2NewMemTable()?;
            } else {
                self.refreshEntryCount();

                // todo 系统调用msync成本很高 减少调用的趟数 只同步变化部分
                self.msync()?;
            }

            anyhow::Ok(())
        };

        _ = commitReq.commitResultSender.send(process());
    }

    /// protected by write mutex
    fn writeChange(&mut self, keyWithTxId: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        assert_eq!(self.immutable, false);

        // should move the current one to immutableMemTables then build a new one
        // if self.posInFile + entryTotalSize >= self.memTableFileMmap.len() {
        //     self.switch2NewMemTable(Some(entryTotalSize))?;
        // }

        // 当前这个的kv对应的memtableEnrty
        let memTableFileEntryHeader = {
            let memTableFileEntryHeader: &mut MemTableFileEntryHeader =
                utils::slice2RefMut(&self.memTableFileMmap[self.posInFile..]);

            memTableFileEntryHeader.keySize = keyWithTxId.len() as u16;

            if let Some(ref val) = value {
                memTableFileEntryHeader.valSize = val.len() as u32;
            }

            memTableFileEntryHeader
        };

        let entryContentMmap = {
            let start = self.posInFile + MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
            let end = self.posInFile + memTableFileEntryHeader.entrySize();

            // macos特殊点
            // 在mmap长度大于文件大小的情况下,在超出文件原始大小的区域追加写入时,若写入位置超过一定程度后会出现BadAccess(通常是SIGBUS)
            // 核心原因与 macOS 对文件映射扩展的严格限制和内存页对齐管理机制
            // macos
            // 仅允许对超出文件大小的区域进行有限的隐式扩展（通常限制在单个内存页内）
            // 若写入位置超出这个范围(例如跨越多个内存页),macOS 不会自动扩展文件,而是直接触发SIGBUS,必须提前通过ftruncate显式扩展文件
            // linux 宽松许多
            // 对MAP_SHARED映射,写入超出文件大小的区域时,会自动隐式扩展文件(通过ftruncate),即使跨多个内存页也不会报错
            //#[cfg(target_os = "macos")]
            {
                if end >= self.currentFileSize {
                    let db = self.db.upgrade().unwrap();
                    let pageSize = db.getHeader().pageSize;

                    // 先另外的copy份不去污染字段的值
                    // 等到set_len()调用成功后再去设置字段
                    let mut currentFileSize = self.currentFileSize;
                    if end >= currentFileSize {
                        // 额外再增加16个os内存页大小,以防止频繁的干这个
                        currentFileSize = end + pageSize * 16;
                    }

                    let memTableFile = unsafe { File::from_raw_fd(self.memTableFileFd) };
                    memTableFile.set_len(currentFileSize as u64)?;
                    mem::forget(memTableFile);

                    self.currentFileSize = currentFileSize;
                }
            }

            &mut self.memTableFileMmap[start..end]
        };

        // 落地 key 到memtable对应的文件
        entryContentMmap[..keyWithTxId.len()].copy_from_slice(keyWithTxId.as_slice());

        // 落地 val 到memtable对应的文件
        if let Some(ref value) = value {
            entryContentMmap[keyWithTxId.len()..].copy_from_slice(value);
        }

        // entryCount不应1个个递增,应该以tx的entryCount为单位来更新的
        // 可能会涉及到多个memTable,如果切换了新的,原来的旧的需要设置entryCount的
        //self.header.entryCount += 1;
        self.writtenEntryCountInTx += 1;
        self.posInFile += memTableFileEntryHeader.entrySize();

        // 变量体系中同步记录
        self.changes.insert(keyWithTxId, value);

        Ok(())
    }

    // 如果memTable大小设置的比较小,1个tx写入的过程会涉及到多趟切换到新的memTable
    // 是不是可以单个tx内容写入的过程中不会切换到newMemTable,等到全部写完了再看是不是超过大小了来确定要不要切换的
    fn switch2NewMemTable(&mut self) -> Result<()> {
        let db = self.db.upgrade().unwrap();

        self.refreshEntryCount();
        self.msync()?;

        let newMemTableFileSize = db.dbOption.memTableMaxSize + MEM_TABLE_FILE_HEADER_SIZE;

        // open a new memTable
        // 测试得知当设置memTable大小为1024byte时候,以下的action要消耗4ms的
        let newMemTable = {
            let mutableMemTableFilePath =
                Path::join(db.dbOption.dirPath.as_ref(),
                           format!("{}.{}", self.memTableFileNum + 1, MEM_TABLE_FILE_EXTENSION));

            let mut newMemTable = MemTable::open(mutableMemTableFilePath, newMemTableFileSize)?;
            newMemTable.db = Arc::downgrade(&db);

            newMemTable
        };

        // 使用新的替换旧的memeTable,换下来的旧的变为immutableMemTable收集
        // see RWLock::replace
        // 目前memTableOld是孤魂野鬼状态,既不是唯一的那个mutableMemTable,也不是immutableMemTable
        // 如果在添加到immutableMemTables之前发生了故障应该如何应对?
        // 问题的核心是文件中还有别的tx的内容
        // 应该先早点将memTableOld添加到immutableMemTables的
        let mut memTableOld = mem::replace(self, newMemTable);
        memTableOld.immutable = true;

        // memTableOld当前已封版,变化为MemTableR发送到另外的thread
        let fd = memTableOld.memTableFileFd;

        // memTableOld添加到immutableMemTables中
        {
            let mut immutableMemTables = db.immutableMemTables.write().unwrap();

            // 趁着获取write锁的时候,筛选和清理掉需要保留的immutableMemTable
            for immutableMemTable in immutableMemTables.drain(..).collect::<Vec<_>>() {
                if immutableMemTable.header.written2Disk {
                    let _ = immutableMemTable.destroy();
                } else {
                    immutableMemTables.push(immutableMemTable);
                }
            }

            immutableMemTables.push(memTableOld);
        }

        let memTableR = MemTableR::try_from(fd)?;
        db.memTableRSender.send(memTableR)?;

        Ok(())
    }

    pub(crate) fn destroy(self) -> Result<()> {
        let memTableFilePath = self.memTableFilePath.clone();
        drop(self);
        fs::remove_file(memTableFilePath)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn msync(&self) -> Result<()> {
        self.memTableFileMmap.flush_range(0, self.posInFile)?;
        Ok(())
    }

    fn refreshEntryCount(&mut self) {
        self.header.entryCount += self.writtenEntryCountInTx as u32;
        self.writtenEntryCountInTx = 0;
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        // close fd before munmap not wrong
        drop(unsafe { File::from_raw_fd(self.memTableFileFd) });
    }
}

pub(crate) fn readEntry(entryData: &[u8]) -> (&[u8], Option<&[u8]>, usize) {
    let memTableFileEntryHeader: &MemTableFileEntryHeader = utils::slice2Ref(entryData);

    // keySize should greater than 0, valSize can be 0
    assert!(memTableFileEntryHeader.keySize > 0);

    let key = {
        let start = MEM_TABLE_FILE_ENTRY_HEADER_SIZE;
        let end = start + memTableFileEntryHeader.keySize as usize;

        &entryData[start..end]
    };

    let value =
        if memTableFileEntryHeader.valSize > 0 {
            let start = MEM_TABLE_FILE_ENTRY_HEADER_SIZE + memTableFileEntryHeader.keySize as usize;
            let end = start + memTableFileEntryHeader.valSize as usize;

            Some(&entryData[start..end])
        } else {
            None
        };

    (key, value, memTableFileEntryHeader.entrySize())
}

pub(crate) const MEM_TABLE_FILE_HEADER_SIZE: usize = size_of::<MemTableFileHeader>();

#[repr(C)]
pub(crate) struct MemTableFileHeader {
    /// 当它是mutable时候 不断写入的时候递增
    pub(crate) entryCount: u32,

    /// 由对应的memTableR写入,当true时候当前这个memTable就可删掉了
    pub(crate) written2Disk: bool,
}

pub(crate) const MEM_TABLE_FILE_ENTRY_HEADER_SIZE: usize = size_of::<MemTableFileEntryHeader>();

/// representation in file <br>
/// keySize u16 | valSize u32 | key | val
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MemTableFileEntryHeader {
    pub(crate) keySize: u16,
    pub(crate) valSize: u32,
}

impl MemTableFileEntryHeader {
    #[inline]
    pub(crate) fn entrySize(&self) -> usize {
        MEM_TABLE_FILE_ENTRY_HEADER_SIZE +
            self.keySize as usize + self.valSize as usize
    }
}

