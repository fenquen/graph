use std::{ptr, usize};
use crate::page_elem::PageElem;
use crate::types::PageId;
use anyhow::Result;

/// 第1个page的header的flag, 它是在dbHeader后边
pub(crate) const PAGE_FLAG_META: u16 = 1;

pub(crate) const PAGE_FLAG_LEAF: u16 = 1 << 1;
pub(crate) const PAGE_FLAG_LEAF_OVERFLOW: u16 = 1 << 2;
pub(crate) const PAGE_FLAG_BRANCH: u16 = 1 << 3;

pub(crate) const PAGE_FLAG_DUMMY: u16 = 1 << 4;

pub(crate) const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();
pub(crate) const PAGE_ID_SIZE: usize = size_of::<PageId>();

pub(crate) const LEAF_ELEM_META_SIZE: usize = size_of::<PageElemMetaLeaf>();
pub(crate) const LEAF_ELEM_OVERFLOW_META_SIZE: usize = size_of::<PageElemMetaLeafOverflow>();
pub(crate) const BRANCH_ELEM_META_SIZE: usize = size_of::<PageElemMetaBranch>();

pub(crate) static PAGE_HEADER_DUMMY_BRANCH: PageHeader = PageHeader {
    id: 0,
    flags: PAGE_FLAG_DUMMY | PAGE_FLAG_BRANCH,
    elemCount: 0,
    nextOverflowPageId: 0,
};

pub(crate) static PAGE_HEADER_DUMMY_LEAF: PageHeader = PageHeader {
    id: 0,
    flags: PAGE_FLAG_DUMMY | PAGE_FLAG_LEAF,
    elemCount: 0,
    nextOverflowPageId: 0,
};

#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageHeader {
    pub(crate) id: PageId,
    pub(crate) flags: u16,
    pub(crate) elemCount: u16,
    pub(crate) nextOverflowPageId: PageId,
}

impl PageHeader {
    #[inline]
    pub(crate) fn isLeaf(&self) -> bool {
        self.flags & PAGE_FLAG_LEAF != 0
    }

    #[inline]
    pub(crate) fn isLeafOverflow(&self) -> bool {
        self.flags & PAGE_FLAG_LEAF_OVERFLOW != 0
    }

    #[inline]
    pub(crate) fn isBranch(&self) -> bool {
        self.flags & PAGE_FLAG_BRANCH != 0
    }

    #[inline]
    pub(crate) fn isDummy(&self) -> bool {
        self.flags & PAGE_FLAG_DUMMY != 0
    }
}

#[macro_export]
macro_rules! impl_read_page_elem_meta {
    ($self:ident, $pageElemMetaType:ty, $index:ident) => {
        {
            let mut ptr = $self as *const _ as *const u8;
            ptr = unsafe { ptr.add(PAGE_HEADER_SIZE) };

            for _ in 0..$index {
                let elem = unsafe { &*(ptr as *const $pageElemMetaType) };
                ptr = unsafe { ptr.add(elem.diskSize()) };
            }

            unsafe { &*(ptr as *const $pageElemMetaType) }
        }
    };
}

impl<'a> PageHeader {
    pub(crate) fn readPageElemMeta(&self, index: usize) -> Result<&dyn PageElemMeta> {
        if self.isLeaf() {
            return Ok(impl_read_page_elem_meta!(self, PageElemMetaLeaf, index));
        }

        if self.isLeafOverflow() {
            return Ok(impl_read_page_elem_meta!(self, PageElemMetaLeafOverflow, index));
        }

        if self.isBranch() {
            return Ok(impl_read_page_elem_meta!(self, PageElemMetaBranch, index));
        }

        throw!("unsupported page header")
    }
}

pub(crate) trait PageElemMeta {
    fn readPageElem(&'_ self) -> PageElem<'_>;

    /// 整个的pageElem大小
    fn diskSize(&self) -> usize;
}

/// 对应 PageElem::Leaf
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemMetaLeaf {
    /// 实际数据离当前的offset
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,

    pub(crate) valueSize: u32,
}

impl PageElemMeta for PageElemMetaLeaf {
    fn readPageElem(&'_ self) -> PageElem<'_> {
        unsafe {
            //const OFFSET: usize = size_of::<u16>() + size_of::<u32>();
            let ptr = (self as *const _ as *const u8).add(LEAF_ELEM_META_SIZE);
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

    fn diskSize(&self) -> usize {
        LEAF_ELEM_META_SIZE +
            self.keySize as usize + self.valueSize as usize
    }
}

/// 对应 PageElem::LeafOverflow
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemMetaLeafOverflow {
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) valPos: usize,
}

impl PageElemMeta for PageElemMetaLeafOverflow {
    fn readPageElem(&'_ self) -> PageElem<'_> {
        unsafe {
            //const OFFSET: usize = size_of::<u16>() + size_of::<usize>();
            let ptr = (self as *const _ as *const u8).add(LEAF_ELEM_OVERFLOW_META_SIZE);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            PageElem::LeafOverflowR(&*key, self.valPos)
        }
    }

    fn diskSize(&self) -> usize {
        LEAF_ELEM_OVERFLOW_META_SIZE + self.keySize as usize
    }
}

/// 对应 PageElem::Branch
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageElemMetaBranch {
    /// 实际数据离当前的offset
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) pageId: PageId,
}

impl PageElemMeta for PageElemMetaBranch {
    fn readPageElem(&'_ self) -> PageElem<'_> {
        unsafe {
            // const OFFSET: usize = size_of::<u16>() + size_of::<PageId>();
            let ptr = (self as *const _ as *const u8).add(BRANCH_ELEM_META_SIZE);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            PageElem::BranchR(&*key, self.pageId)
        }
    }

    fn diskSize(&self) -> usize {
        BRANCH_ELEM_META_SIZE + self.keySize as usize
    }
}