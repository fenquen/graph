use std::{mem, ptr};
use crate::types::PageId;

pub(crate) const PAGE_FLAG_META: u16 = 1;
pub(crate) const PAGE_FLAG_LEAF: u16 = 1 << 1;
pub(crate) const PAGE_FLAG_BRANCH: u16 = 1 << 2;

pub(crate) const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();

pub(crate) const LEAF_ELEM_META_SIZE: usize = size_of::<LeafElemMeta>();
pub(crate) const BRANCH_ELEM_META_SIZE: usize = size_of::<BranchElemMeta>();

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
}

impl<'a> PageHeader {
    pub(crate) fn readLeafElemMeta(&'a self, index: usize) -> &'a LeafElemMeta {
        let mut ptr = self as *const _ as *const u8;
        ptr = unsafe { ptr.add(PAGE_HEADER_SIZE).add(LEAF_ELEM_META_SIZE * index) };

        unsafe { mem::transmute(ptr) }
    }

    pub(crate) fn readBranchElemMeta(&'a self, index: usize) -> &'a BranchElemMeta {
        let mut ptr = self as *const _ as *const u8;
        ptr = unsafe { ptr.add(PAGE_HEADER_SIZE).add(BRANCH_ELEM_META_SIZE * index) };

        unsafe { mem::transmute(ptr) }
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct LeafElemMeta {
    pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) valueSize: u32,
}

impl<'a> LeafElemMeta {
    pub(crate) fn readKV(&'a self) -> (&'a [u8], &'a [u8]) {
        unsafe {
            let ptr = (self as *const _ as *const u8).add(self.offset as usize);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);
            let val = ptr::slice_from_raw_parts(ptr.add(self.keySize as usize), self.valueSize as usize);

            (&*key, &*val)
        }
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct BranchElemMeta {
    pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) pageId: PageId,
}

impl<'a> BranchElemMeta {
    pub(crate) fn readKey(&'a self) -> (&'a [u8]) {
        unsafe {
            let ptr = (self as *const _ as *const u8).add(self.offset as usize);
            let key = ptr::slice_from_raw_parts(ptr, self.keySize as usize);
            &*key
        }
    }
}