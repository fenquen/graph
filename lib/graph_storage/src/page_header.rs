use std::ptr;
use crate::page_elem::PageElem;
use crate::types::PageId;
use anyhow::Result;

/// 从未非配过的page/分配后又回收的page
pub(crate) const PAGE_FLAG_INVALID: u16 = 0;

/// page0(保存dbHeader)的flag
pub(crate) const PAGE_FLAG_META: u16 = 1;
pub(crate) const PAGE_FLAG_LEAF: u16 = 1 << 1;
pub(crate) const PAGE_FLAG_LEAF_OVERFLOW: u16 = 1 << 2;
pub(crate) const PAGE_FLAG_BRANCH: u16 = 1 << 3;

pub(crate) const PAGE_FLAG_DUMMY: u16 = 1 << 4;
/// 说明这个page可以被free回收
pub(crate) const PAGE_FLAG_FREEABLE: u16 = 1 << 6;

pub(crate) const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();
pub(crate) const PAGE_ID_SIZE: usize = size_of::<PageId>();

pub(crate) const LEAF_ELEM_META_SIZE: usize = size_of::<PageElemHeaderLeaf>();
pub(crate) const LEAF_ELEM_OVERFLOW_META_SIZE: usize = size_of::<PageElemHeaderLeafOverflow>();
pub(crate) const BRANCH_ELEM_META_SIZE: usize = size_of::<PageElemHeaderBranch>();

#[derive(Copy, Clone, Default)]
#[repr(C)]
pub(crate) struct PageHeader {
    pub(crate) id: PageId,

    /// prevPageId和nextPageId 如果是0可以起到none效果
    /// 因为id是0的那个page是不参与数据保存的
    /// 而且db刚init时候的rootPageId是1
    pub(crate) prevPageId: PageId,
    pub(crate) nextPageId: PageId,

    pub(crate) parentPageId: PageId,

    /// none 说明db当前只有1个leaf page
    pub(crate) indexInParentPage: Option<usize>,

    pub(crate) flags: u16,

    /// 如果是0的话 可能是1个新生成的
    /// 也有可能是因为和别的page合并空掉了
    /// 也有可能是因为delete而变空了的
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

    /// 虽然说是reset,还是保留了pageId
    pub(crate) fn reset(&mut self) {
        let id = self.id;
        *self = Default::default();
        self.id = id;
    }

    #[inline]
    pub(crate) fn isFreeable(&self) -> bool {
        self.flags & PAGE_FLAG_FREEABLE != 0
    }

    #[inline]
    pub(crate) fn markFreeable(&mut self) {
        self.flags |= PAGE_FLAG_FREEABLE;
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
    pub(crate) fn readPageElemHeader(&self, index: usize) -> Result<&dyn PageElemHeader> {
        if self.isLeaf() {
            return Ok(impl_read_page_elem_meta!(self, PageElemHeaderLeaf, index));
        }

        if self.isLeafOverflow() {
            return Ok(impl_read_page_elem_meta!(self, PageElemHeaderLeafOverflow, index));
        }

        if self.isBranch() {
            return Ok(impl_read_page_elem_meta!(self, PageElemHeaderBranch, index));
        }

        throw!("unsupported page header")
    }
}

/// 和MemTableFileEntryHeader像
pub(crate) trait PageElemHeader {
    fn readPageElem(&'_ self) -> PageElem<'_>;

    /// 整个的pageElem大小
    fn diskSize(&self) -> usize;
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

impl PageElemHeader for PageElemHeaderLeaf {
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
pub(crate) struct PageElemHeaderLeafOverflow {
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) valPos: usize,
}

impl PageElemHeader for PageElemHeaderLeafOverflow {
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
pub(crate) struct PageElemHeaderBranch {
    /// 实际数据离当前的offset
    // pub(crate) offset: u16,
    pub(crate) keySize: u16,
    pub(crate) pageId: PageId,
}

impl PageElemHeader for PageElemHeaderBranch {
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