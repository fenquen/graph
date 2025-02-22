use crate::page_header::PageHeader;
use crate::utils;
use memmap2::Mmap;
use std::sync::Arc;
use crate::types::PageId;
use anyhow::Result;

/// page presentation in program
pub(crate) struct Page {
    pub(crate) parentPage: Option<Arc<Page>>,
    pub(crate) mmap: Mmap,
    pub(crate) header: &'static PageHeader,
    pub(crate) elems: Vec<PageElem<'static>>,
    pub(crate) childPages: Option<Vec<Arc<Page>>>,
}

// pub(crate) fn
impl Page {
    pub(crate) fn readFromPageHeader(mmap: Mmap) -> Page {
        let pageHeader =   utils::slice2Ref::<PageHeader>(&mmap) ;

        let elems = {
            let mut elems = Vec::with_capacity(pageHeader.elemCount as usize);

            for a in 0..pageHeader.elemCount as usize {
                let elem =
                    if pageHeader.isLeaf() {
                        let leafElemMeta = pageHeader.readLeafElemMeta(a);
                        let (key, val) = leafElemMeta.readKV();
                        PageElem::LeafR(key, val)
                    } else {
                        let branchElemMeta = pageHeader.readBranchElemMeta(a);
                        let (key, pageId) = branchElemMeta.readKey();
                        PageElem::BranchR(key, pageId)
                    };

                elems.push(elem);
            }

            elems
        };

        Page {
            parentPage: None,
            mmap,
            header: pageHeader,
            elems,
            childPages: None,
        }
    }

    #[inline]
    pub(crate) fn isLeaf(&self) -> bool {
        self.header.isLeaf()
    }
}

// table(文件)->block   block(文件)->page
pub(crate) enum PageElem<'a> {
    /// key is with txId
    LeafR(&'a [u8], &'a [u8]),

    /// key is not with txId
    BranchR(&'a [u8], PageId),
}

impl<'a> PageElem<'a> {
    pub(crate) fn isLeaf(&self) -> bool {
        match self {
            PageElem::LeafR(_, _) => true,
            PageElem::BranchR(_, _) => false,
        }
    }

    pub(crate) fn asBranchR(&self) -> Result<(&'a [u8], PageId)> {
        if let PageElem::BranchR(keyWithoutTxId, pageId) = self {
            Ok((keyWithoutTxId, *pageId))
        } else {
            throw!("a")
        }
    }
}