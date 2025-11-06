use std::{fs, mem};
use std::fs::OpenOptions;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Weak;
use crate::bitmap::BtreeBitmap;
use crate::db::DB;
use crate::types::PageId;
use crate::utils;
use anyhow::Result;
use memmap2::MmapMut;

pub(crate) struct PageAllocatorWrapper {
    pageAllocator: PageAllocator,
    fileMmap: MmapMut,
    fd: RawFd,
}

impl PageAllocatorWrapper {
    /// 和memTable的open相像
    pub(crate) fn open(pageAllocatorFilePath: impl AsRef<Path>, maxOrder: u8) -> Result<PageAllocatorWrapper> {
        let fileExist = fs::exists(&pageAllocatorFilePath)?;

        let file = {
            let mut openOptions = OpenOptions::new();
            openOptions.read(true).write(true).create(true);

            openOptions.open(&pageAllocatorFilePath)?
        };

        let fd = file.as_raw_fd();

        // 对应的序列化文件已存在 deserialize
        let (pageAllocator, fileMmap) =
            if fileExist {
                let fileLen = file.metadata()?.len() as usize;

                let fileMmap = utils::mmapFdMut(fd, None, Some(fileLen))?;
                let pageAllocator = PageAllocator::deserialize(fileMmap.as_ref());

                (pageAllocator, fileMmap)
            } else { // 对应的序列化文件不存在,需要生成空的patternPerOrder,然后序列化写入到文件
                let pageAllocator = PageAllocator::new(maxOrder);
                let binary = pageAllocator.serialize();

                file.set_len(binary.len() as u64)?;
                file.sync_all()?;

                let mut fileMmap = utils::mmapFdMut(fd, None, Some(binary.len()))?;
                fileMmap.as_mut().copy_from_slice(&binary);

                (pageAllocator, fileMmap)
            };

        mem::forget(file);

        Ok(PageAllocatorWrapper {
            pageAllocator,
            fileMmap,
            fd,
        })
    }

    /// 当tx结束后更新page的分配情况到底部的文件
    /// 要是偷懒的话其实用serialize也能足够,不过serialize会单独生成Vec<u8>,然后还要copy到mmap还是有点罗嗦的
    /// 可以直接1步到位写到mmap的
    pub(crate) fn refresh(&mut self) {
        self.pageAllocator.refresh(self.fileMmap.as_mut());
    }
}

impl Deref for PageAllocatorWrapper {
    type Target = PageAllocator;

    fn deref(&self) -> &Self::Target {
        &self.pageAllocator
    }
}

impl DerefMut for PageAllocatorWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pageAllocator
    }
}

pub(crate) struct PageAllocator {
    patternPerOrder: Vec<BtreeBitmap>,
    // memTable也是这样的
    pub(crate) db: Weak<DB>,
    maxOrder: u8,
}

impl PageAllocator {
    pub(crate) fn new(maxOrder: u8) -> PageAllocator {
        let mut pageAllocator = PageAllocator {
            patternPerOrder: Vec::with_capacity(maxOrder as usize + 1),
            db: Weak::new(),
            maxOrder,
        };

        let mut pageCount = 2usize.pow(maxOrder as u32);

        for _ in 0..=maxOrder {
            pageAllocator.patternPerOrder.push(BtreeBitmap::new(pageCount));
            pageCount = pageCount / 2;
        }

        pageAllocator
    }

    // 要分配的page数量对应的order是2,在这个order分配到的indexInOrder是2
    //
    // order 0  0 0 0 0 0 0 0 0 1 1 1 1  起始的index indexInOrder*2^(order-0)是8,数量2^(order-0)
    // order 1  00 00 00 00 11 11        起始的index indexInOrder*2^(order-1)是4,数量2^(order-1)
    // order 2  0000 0000 1111
    // order 3  00000000 11110000        起始的index indexInOrder/(2^(3-order))是0,数量不满1个
    pub(crate) fn allocate(&mut self, expectSize: usize, pageSize: usize) -> Option<(PageId, usize)> {
        let targetOrder = {
            //let pageSize = self.db.upgrade().unwrap().getHeader().pageSize;
            let expectSize = utils::roundUp2Multiple(expectSize, pageSize);
            utils::ceilLog2(expectSize / pageSize)
        };

        let indexUnderTargetOrder = &mut self.patternPerOrder[targetOrder].alloc();
        if indexUnderTargetOrder.is_none() {
            return None;
        }

        let indexUnderTargetOrder = indexUnderTargetOrder.unwrap();

        // 比它小的各个order的bitmap
        for subOrder in 0..targetOrder {
            let subOrderPattern = &mut self.patternPerOrder[subOrder];

            // 比它小的各个order的bitmap的elemIndex
            let mut indexUnderOrder = indexUnderTargetOrder * 2usize.pow((targetOrder - subOrder) as u32);
            subOrderPattern.set(indexUnderOrder);

            // 比它小的各个order的对应的数量,包含了上边的那个indexUnderOrder本身
            // 越往1个小order要乘以2
            for _ in 1..2usize.pow((targetOrder - subOrder) as u32) {
                indexUnderOrder += 1;
                subOrderPattern.set(indexUnderOrder);
            }
        }

        // 比它大的各个order的bitmap的elemIndex
        for superiorOrder in targetOrder + 1..=self.maxOrder as usize {
            let indexUnderSuperiorOrder = indexUnderTargetOrder / (2usize.pow((superiorOrder - targetOrder) as u32));
            (&mut self.patternPerOrder[superiorOrder]).set(indexUnderSuperiorOrder);
        }

        let pageId = {
            let mut index = indexUnderTargetOrder;
            for _ in 0..targetOrder {
                index *= 2;
            }

            index as PageId
        };

        Some((pageId, 2usize.pow(targetOrder as u32)))
    }

    pub(crate) fn free(&mut self, pageId: PageId, count: usize) {
        let targetOrder = utils::ceilLog2(count);

        let indexUnderTargetOrder = {
            let mut index = pageId as usize;

            // indexUnderTargetOrder 需要不断的除以2
            for _ in 0..targetOrder {
                index = index / 2;
            }

            index
        };

        (&mut self.patternPerOrder[targetOrder]).clear(indexUnderTargetOrder);

        // 下级各个order的bitmap,这部分和allocate()那边的相同
        for subOrder in 0..targetOrder {
            let orderPattern = &mut self.patternPerOrder[subOrder];

            // 比它小的各个order的bitmap的elemIndex
            let mut indexUnderOrder = indexUnderTargetOrder * 2usize.pow((targetOrder - subOrder) as u32);
            orderPattern.clear(indexUnderOrder);

            // 比它小的各个order的对应的数量,包含了上边的那个indexUnderOrder本身
            // 越往1个小order要乘以2
            for _ in 1..2usize.pow((targetOrder - subOrder) as u32) {
                indexUnderOrder += 1;
                orderPattern.clear(indexUnderOrder);
            }
        }

        // 上级各个order的bitmap的elemIndex,这部分和allocate()那边的不同了
        // 映射到最底部如果都是0那么clear,不断的向上的
        let mut index = indexUnderTargetOrder;
        for superiorOrder in targetOrder + 1..=self.maxOrder as usize {
            let directChildOrderPattern = &self.patternPerOrder[superiorOrder - 1];

            // 上级的1个元素对应下级的2个元素
            // 如果下级的2个元素不都是干净的,那它也不是的
            if directChildOrderPattern.get(index) ||
                directChildOrderPattern.get(index + 1) {
                break;
            }

            index = index / 2;

            (&mut self.patternPerOrder[superiorOrder]).clear(index);
        }
    }

    const MAX_ORDER_BYTE_LEN: usize = size_of::<u8>();
    const ORDER_PATTERN_BINARY_LEN_BYTE_LEN: usize = size_of::<u32>();

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut binary = Vec::new();

        // 1字节 maxOrder
        binary.extend(self.maxOrder.to_be_bytes());

        for orderPattern in &self.patternPerOrder {
            let orderPatternBinary = orderPattern.serialize();

            // 4字节 orderPatternBinary 长度
            binary.extend((orderPatternBinary.len() as u32).to_be_bytes());
            binary.extend(orderPatternBinary);
        }

        binary
    }

    pub(crate) fn refresh(&self, dest: &mut [u8]) {
        let mut currentPos = Self::MAX_ORDER_BYTE_LEN;

        for orderPattern in &self.patternPerOrder {
            // 读取当前这个orderPattern的binary长度
            let orderPatternBinaryLen = u32::from_be_bytes(dest[currentPos..currentPos + Self::ORDER_PATTERN_BINARY_LEN_BYTE_LEN].try_into().unwrap());
            currentPos += Self::ORDER_PATTERN_BINARY_LEN_BYTE_LEN;

            orderPattern.refresh(&mut dest[currentPos..currentPos + orderPatternBinaryLen as usize]);
            currentPos += orderPatternBinaryLen as usize;
        }
    }

    pub(crate) fn deserialize(binary: &[u8]) -> PageAllocator {
        let mut currentPos = 0usize;

        // 读取1字节 maxOrder
        let maxOrder = u8::from_be_bytes(binary[0..Self::MAX_ORDER_BYTE_LEN].try_into().unwrap());
        currentPos += Self::MAX_ORDER_BYTE_LEN;

        let mut patternPerOrder = Vec::with_capacity(maxOrder as usize + 1);

        for _ in 0..=maxOrder {
            // 读取4字节 orderPatternBinary 长度
            let orderPatternBinaryLen = u32::from_be_bytes(binary[currentPos..currentPos + Self::ORDER_PATTERN_BINARY_LEN_BYTE_LEN].try_into().unwrap());
            currentPos += Self::ORDER_PATTERN_BINARY_LEN_BYTE_LEN;

            let orderPattern = BtreeBitmap::deserialize(&binary[currentPos..currentPos + orderPatternBinaryLen as usize]);
            currentPos += orderPatternBinaryLen as usize;
            patternPerOrder.push(orderPattern);
        }

        PageAllocator {
            patternPerOrder,
            db: Weak::new(),
            maxOrder,
        }
    }
}