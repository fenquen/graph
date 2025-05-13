use std::{mem, ptr, usize};
use crate::page::PageElem;
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
    pageId: 0,
    flags: PAGE_FLAG_DUMMY | PAGE_FLAG_BRANCH,
    elemCount: 0,
    nextOverflowPageId: 0,
};

pub(crate) static PAGE_HEADER_DUMMY_LEAF: PageHeader = PageHeader {
    pageId: 0,
    flags: PAGE_FLAG_DUMMY | PAGE_FLAG_LEAF,
    elemCount: 0,
    nextOverflowPageId: 0,
};

#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct PageHeader {
    pub(crate) pageId: PageId,
    pub(crate) flags: u16,
    pub(crate) elemCount: u16,
    /// exist when larger than 0
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

impl<'a> PageHeader {
    pub(crate) fn readPageElemMeta(&self, index: usize) -> Result<&dyn PageElemMeta> {
        if self.isLeaf() {
            let leafElemMeta = self.readPageElemMetaLeaf(index);
            return Ok(leafElemMeta as &dyn PageElemMeta);
        }

        if self.isLeafOverflow() {
            let leafOverflowElemMeta = self.readPageElemMetaLeafOverflow(index);
            return Ok(leafOverflowElemMeta);
        }

        if self.isBranch() {
            let branchElemMeta = self.readPageElemMetaBranch(index);
            return Ok(branchElemMeta);
        }

        throw!("unsupported page header")
    }

    fn readPageElemMetaLeaf(&'a self, index: usize) -> &'a PageElemMetaLeaf {
        let mut ptr = self as *const _ as *const u8;
        ptr = unsafe { ptr.add(PAGE_HEADER_SIZE).add(LEAF_ELEM_META_SIZE * index) };
        unsafe { mem::transmute(ptr) }
    }

    fn readPageElemMetaLeafOverflow(&'a self, index: usize) -> &'a PageElemMetaLeafOverflow {
        let mut ptr = self as *const _ as *const u8;
        ptr = unsafe { ptr.add(PAGE_HEADER_SIZE).add(LEAF_ELEM_OVERFLOW_META_SIZE * index) };

        unsafe { mem::transmute(ptr) }
    }

    fn readPageElemMetaBranch(&'a self, index: usize) -> &'a PageElemMetaBranch {
        let mut ptr = self as *const _ as *const u8;
        ptr = unsafe { ptr.add(PAGE_HEADER_SIZE).add(BRANCH_ELEM_META_SIZE * index) };

        unsafe { mem::transmute(ptr) }
    }
}

pub(crate) trait PageElemMeta {
    fn readPageElem(&self) -> PageElem;
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
    fn readPageElem(&self) -> PageElem {
        unsafe {
            let ptr = (self as *const _ as *const u8);//.add(self.offset as usize);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);
            let val = ptr::slice_from_raw_parts(ptr.add(self.keySize as usize), self.valueSize as usize);

            PageElem::LeafR(&*key, &*val)
        }
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
    fn readPageElem(&self) -> PageElem {
        unsafe {
            let ptr = (self as *const _ as *const u8);//.add(self.offset as usize);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            let valPos = {
                let valPos = ptr::slice_from_raw_parts(ptr.add(self.keySize as usize), size_of::<usize>());
                usize::from_be_bytes(*(valPos as *const _))
            };

            PageElem::LeafOverflowR(&*key, valPos)
        }
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
    fn readPageElem(&self) -> PageElem {
        unsafe {
            let ptr = (self as *const _ as *const u8);//.add(self.offset as usize);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);

            let pageId = {
                let pageId = ptr::slice_from_raw_parts(ptr.add(self.keySize as usize), PAGE_ID_SIZE);
                PageId::from_be_bytes(*(pageId as *const _))
            };

            PageElem::BranchR(&*key, pageId)
        }
    }
}