use std::sync::{Arc, RwLock};
use dashmap::mapref::one::RefMut;
use memmap2::Mmap;
use crate::page_header::PageHeader;
use crate::types::PageId;

/// page presentation in program
pub(crate) struct Page {
    pub(crate) parentPage: Option<Arc<RwLock<Page>>>,
    pub(crate) mmap: Mmap,
    pub(crate) header: &'static PageHeader,
    pub(crate) elems: Vec<PageElem<'static>>,
    pub(crate) childPages: Option<Vec<Arc<RwLock<Page>>>>,
}

impl Page {
    pub(crate) fn readFromPageHeader(mmap: Mmap, pageHeader: &'static PageHeader) -> Page {
        let elems = {
            let mut elems = Vec::with_capacity(pageHeader.elemCount as usize);

            for a in 0..pageHeader.elemCount as usize {
                let elem =
                    if pageHeader.isLeaf() {
                        let leafElemMeta = pageHeader.readLeafElemMeta(a);
                        let (k, v) = leafElemMeta.readKV();
                        PageElem::LeafR(k, v)
                    } else {
                        let branchElemMeta = pageHeader.readBranchElemMeta(a);
                        PageElem::BranchR(branchElemMeta.readKey())
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
}

// table(文件)->block   block(文件)->page
pub(crate) enum PageElem<'a> {
    LeafR(&'a [u8], &'a [u8]),
    LeafRW(Vec<u8>, Vec<u8>),
    BranchR(&'a [u8]),
    BranchRW(Vec<u8>),
}

impl<'a> PageElem<'a> {
    pub(crate) fn isLeaf(&self) -> bool {
        match self {
            PageElem::LeafR(_, _) | PageElem::LeafRW(_, _) => true,
            PageElem::BranchR(_) | PageElem::BranchRW(_) => false,
        }
    }
}