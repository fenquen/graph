use std::ptr;
use crate::page_elem::PageElem;
use crate::types::PageId;
use crate::utils;
use crate::utils::Codec;

/// 和MemTableFileEntryHeader像
pub(crate) trait PageElemHeader {
    fn readPageElem<'a>(&self, src: &'a [u8]) -> PageElem<'a>;

    /// 整个的pageElem大小
    fn elemTotalSize(&self) -> usize;
}

/// 对应 PageElem::Leaf
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemHeaderLeaf {
    /// 实际数据离当前的offset
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,

    pub(crate) valueSize: u32,
}

impl PageElemHeaderLeaf {
    pub(crate) const fn size() -> usize {
        size_of::<u16>() + size_of::<u32>()
    }
}

impl Codec for PageElemHeaderLeaf {
    fn serializeTo(&self, dest: &mut [u8]) {
        let mut position = 0usize;

        utils::writeNum(self.keySize, &mut dest[position..]);
        position += size_of_val(&self.keySize);

        utils::writeNum(self.valueSize, &mut dest[position..]);
    }

    fn deserializeFrom(src: &[u8]) -> Self {
        let mut position = 0usize;

        let keySize = utils::readNum(&src[position..]);
        position += size_of::<u16>();

        let valueSize = utils::readNum(&src[position..]);

        Self {
            keySize,
            valueSize,
        }
    }
}

impl PageElemHeader for PageElemHeaderLeaf {
    fn readPageElem<'a>(&self, src: &'a [u8]) -> PageElem<'a> {
        unsafe {
            //const OFFSET: usize = size_of::<u16>() + size_of::<u32>();
            let ptr = (src as *const _ as *const u8).add(PageElemHeaderLeaf::size());
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            let value =
                if self.valueSize > 0 {
                    let value = ptr::slice_from_raw_parts(ptr.add(self.keySize as usize), self.valueSize as usize);
                    Some(&*value)
                } else {
                    None
                };

            PageElem::LeafR(&*key, value)
        }
    }

    fn elemTotalSize(&self) -> usize {
        PageElemHeaderLeaf::size() + self.keySize as usize + self.valueSize as usize
    }
}

// ------------------------------------------------------------------------------------------------

/// 对应 PageElem::LeafOverflow
// todo leaf overflow 是不是可以和leaf都在相同的page
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemHeaderLeafOverflow {
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) valuePos: usize,
}

impl PageElemHeaderLeafOverflow {
    pub(crate) const fn size() -> usize {
        size_of::<u16>() + size_of::<usize>()
    }
}

impl Codec for PageElemHeaderLeafOverflow {
    fn serializeTo(&self, dest: &mut [u8]) {
        let mut position = 0usize;

        utils::writeNum(self.keySize, &mut dest[position..]);
        position += size_of_val(&self.keySize);

        utils::writeNum(self.valuePos, &mut dest[position..]);
    }

    fn deserializeFrom(src: &[u8]) -> Self {
        let mut position = 0usize;

        let keySize = utils::readNum(&src[position..]);
        position += size_of::<u16>();

        let valPos = utils::readNum(&src[position..]);

        Self {
            keySize,
            valuePos: valPos,
        }
    }
}

impl PageElemHeader for PageElemHeaderLeafOverflow {
    fn readPageElem<'a>(&self, src: &'a [u8]) -> PageElem<'a> {
        unsafe {
            //const OFFSET: usize = size_of::<u16>() + size_of::<usize>();
            let ptr = (src as *const _ as *const u8).add(PageElemHeaderLeafOverflow::size());
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            PageElem::LeafOverflowR(&*key, self.valuePos)
        }
    }


    fn elemTotalSize(&self) -> usize {
        PageElemHeaderLeafOverflow::size() + self.keySize as usize
    }
}

// --------------------------------------------------------------------------------------------

/// 对应 PageElem::Branch
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemHeaderBranch {
    /// 实际数据离当前的offset
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) pageId: PageId,
}

impl PageElemHeaderBranch {
    pub(crate) const fn size() -> usize {
        size_of::<u16>() + size_of::<PageId>()
    }
}

impl Codec for PageElemHeaderBranch {
    fn serializeTo(&self, dest: &mut [u8]) {
        let mut position = 0usize;

        utils::writeNum(self.keySize, &mut dest[position..]);
        position += size_of_val(&self.keySize);

        utils::writeNum(self.pageId, &mut dest[position..]);
    }

    fn deserializeFrom(src: &[u8]) -> Self {
        let mut position = 0usize;

        let keySize = utils::readNum(&src[position..]);
        position += size_of::<u16>();

        let pageId = utils::readNum(&src[position..]);

        Self {
            keySize,
            pageId,
        }
    }
}

impl PageElemHeader for PageElemHeaderBranch {
    fn readPageElem<'a>(&self, src: &'a [u8]) -> PageElem<'a> {
        unsafe {
            // const OFFSET: usize = size_of::<u16>() + size_of::<PageId>();
            let ptr = (src as *const _ as *const u8).add(PageElemHeaderBranch::size());
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            PageElem::BranchR(&*key, self.pageId)
        }
    }

    fn elemTotalSize(&self) -> usize {
        PageElemHeaderBranch::size() + self.keySize as usize
    }
}