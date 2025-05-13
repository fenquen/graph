use std::io::Write;
use crate::page_header::{PageElemMeta, PageHeader};
use crate::{page_header, utils};
use memmap2::{MmapMut};
use std::sync::{Arc, RwLock};
use crate::types::PageId;
use anyhow::Result;

pub(crate) const OVERFLOW_DIVIDE: usize = 100;

/// page presentation in memory
pub(crate) struct Page {
    pub(crate) parentPage: Option<Arc<RwLock<Page>>>,
    /// if page is dummy then it is none
    pub(crate) mmapMut: Option<MmapMut>,
    // 无中生有通过mmap得到的
    pub(crate) header: &'static PageHeader,
    pub(crate) pageElems: Vec<PageElem<'static>>,
    pub(crate) childPages: Option<Vec<Arc<Page>>>,
    //pub(crate) dirty: bool,
}

// pub(crate) fn
impl Page {
    pub(crate) fn readFromMmap(mmapMut: MmapMut) -> Result<Page> {
        let pageHeader = utils::slice2Ref::<PageHeader>(&mmapMut);

        let pageElemVec = {
            let mut pageElemVec = Vec::with_capacity(pageHeader.elemCount as usize);

            for a in 0..pageHeader.elemCount as usize {
                let pageElemMeta = pageHeader.readPageElemMeta(a)?;
                let pageElem = pageElemMeta.readPageElem();
                pageElemVec.push(pageElem);
            }

            pageElemVec
        };

        Ok(Page {
            parentPage: None,
            mmapMut: Some(mmapMut),
            header: pageHeader,
            pageElems: pageElemVec,
            childPages: None,
        })
    }

    pub(crate) fn buildDummyLeafPage() -> Page {
        Page {
            parentPage: None,
            mmapMut: None,
            header: &page_header::PAGE_HEADER_DUMMY_LEAF,
            pageElems: vec![],
            childPages: None,
        }
    }

    #[inline]
    pub(crate) fn isLeaf(&self) -> bool {
        self.header.isLeaf()
    }

    #[inline]
    pub(crate) fn isDummy(&self) -> bool {
        self.header.isDummy()
    }

    /// 计算当前page含有的内容需要用掉多少page
    pub(crate) fn diskSize(&self) -> usize {
        let mut size = page_header::PAGE_HEADER_SIZE;

        for pageElem in &self.pageElems {
            size += pageElem.diskSize();
        }

        size
    }
}

/// table(文件)->block   block(文件)->page
///
/// 目前对leaf节点的保存思路如下
/// 如果value比较小 kv可以1道保存
/// 如果value比较大(那多少算是大呢,目前暂时定为pageSize的25%) value保存到单独的文件 leaf节点本身保存data的pos的
pub(crate) enum PageElem<'a> {
    /// key is with txId
    LeafR(&'a [u8], &'a [u8]),
    /// (key, value在文件中的位置的)
    LeafOverflowR(&'a [u8], usize),

    /// key is with txId
    BranchR(&'a [u8], PageId),

    Dummy4PutLeaf(Vec<u8>, Vec<u8>),
    /// (key, pos, val)
    Dummy4PutLeafOverflow(Vec<u8>, usize, Vec<u8>),

    Dummy4PutBranch(Vec<u8>, Arc<RwLock<Page>>),
}

impl<'a> PageElem<'a> {
    pub(crate) fn asBranchR(&self) -> Result<(&'a [u8], PageId)> {
        if let PageElem::BranchR(keyWithoutTxId, pageId) = self {
            return Ok((keyWithoutTxId, *pageId));
        }

        throw!("a")
    }

    pub(crate) fn write2Disk(&self, dest: &mut [u8]) -> Result<()> {
        // 变为vec 这样的只要不断的push便可以了
        let mut vec = unsafe { Vec::from_raw_parts(dest as *const _ as *mut u8, 0, dest.len()) };

        match self {
            PageElem::LeafR(k, v) => {
                dest.copy_from_slice(k);
            }
            _ => todo!()
        }
        Ok(())
    }

    /// 含有 pageElemMeta
    pub(crate) fn diskSize(&self) -> usize {
        match self {
            PageElem::LeafR(k, v) => page_header::LEAF_ELEM_META_SIZE + k.len() + v.len(),
            PageElem::Dummy4PutLeaf(k, v) => page_header::LEAF_ELEM_META_SIZE + k.len() + v.len(),
            //
            PageElem::LeafOverflowR(k, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            PageElem::Dummy4PutLeafOverflow(k, _, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            //
            PageElem::BranchR(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len() + size_of::<PageId>(),
            PageElem::Dummy4PutBranch(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len() + size_of::<PageId>(),
        }
    }
}