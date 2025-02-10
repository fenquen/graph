use crate::page::Elem::{Branch, Leaf};
use crate::page_header::PageHeader;
use crate::types::PageId;

/// page presentation in program
pub(crate) struct Page<'a> {
    pub(crate) pageId: PageId,
    pub(crate) flags: u16,
    pub(crate) elems: Vec<Elem<'a>>,
}

impl<'a> Page<'a> {
    pub(crate) fn readFromPageHeader(pageHeader: &'a PageHeader) -> Self {
        let elems = {
            let mut elems = Vec::with_capacity(pageHeader.elemCount as usize);

            for a in 0..pageHeader.elemCount as usize {
                let elem =
                    if pageHeader.isLeaf() {
                        let leafElemMeta = pageHeader.readLeafElemMeta(a);
                        let (k, v) = leafElemMeta.readKV();
                        Elem::Leaf(k, v)
                    } else {
                        let branchElemMeta = pageHeader.readBranchElemMeta(a);
                        Elem::Branch(branchElemMeta.readKey())
                    };

                elems.push(elem);
            }

            elems
        };
        
        Page {
            pageId: pageHeader.pageId,
            flags: pageHeader.flags,
            elems,
        }
    }
}

pub(crate) enum Elem<'a> {
    Leaf(&'a [u8], &'a [u8]),
    Branch(&'a [u8]),
}

impl<'a> Elem<'a> {
    pub(crate) fn isLeaf(&self) -> bool {
        match self {
            Leaf { .. } => true,
            Branch(_) => false,
        }
    }
}