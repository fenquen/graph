use crate::page::{Page, PageElem};
use crate::tx::Tx;
use anyhow::Result;
use std::sync::Arc;
use crate::{tx, utils};

pub struct Cursor<'tx> {
    pub(crate) tx: &'tx Tx,

    /// currentPage currentIndexInPage
    stack: Vec<(Arc<Page>, usize)>,
}

// pub fn
impl<'tx> Cursor<'tx> {
    pub fn new(tx: &'tx Tx) -> Result<Cursor<'tx>> {
        let dbHeader = tx.db.getHeader();
        let rootPage = tx.db.getPageById(dbHeader.rootPageId, None)?;

        Ok(Cursor {
            tx,
            stack: vec![(rootPage, 0)],
        })
    }
}

// pub (crate) fn
impl<'tx> Cursor<'tx> {
    pub(crate) fn seek(&mut self, keyWithoutTxId: &[u8]) -> Result<()> {
        let tx = self.tx;
        let stackLen = self.stack.len();

        let (currentPage, currentIndexInPage) = self.stackTopMut();

        // try to locate the index in page
        if currentPage.header.isLeaf() {
            let keyWithtxId = tx::appendKeyWithTxId(keyWithoutTxId, tx.id);

            // returns the the index of minimal value which is greater or equal with the search value
            // if there is an equivalent value ,then returns Ok(index) else returns Err(index)
            let index =
                currentPage.elems.binary_search_by(|pageElem| {
                    match pageElem {
                        PageElem::LeafR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&keyWithtxId.as_slice()),
                        _ => panic!("impossible")
                    }
                }).unwrap_or_else(|index| {
                    // means that there is no equivalant key
                    // the index represents the minial key larger than target key
                    if index == 0 {
                        0
                    } else {
                        index - 1
                    }
                });

            *currentIndexInPage = index;
        } else {
            let index =
                currentPage.elems.binary_search_by(|pageElem| {
                    match pageElem {
                        PageElem::BranchR(keyWithoutTxIdInElem, _) => keyWithoutTxIdInElem.cmp(&keyWithoutTxId),
                        _ => panic!("impossible")
                    }
                }).unwrap_or_else(|index| {
                    // none, the target has no possibility in the descendant
                    // even thought ,we still need to go on
                    if index >= stackLen {
                        stackLen - 1
                    } else {
                        index
                    }
                });

            *currentIndexInPage = index;

            let pageElem = currentPage.elems.get(index).unwrap();

            let (keyWithoutTxIdInElem, pageId) = pageElem.asBranchR()?;
            assert_eq!(keyWithoutTxIdInElem, keyWithoutTxId);

            let page = tx.db.getPageById(pageId, Some(currentPage.clone()))?;
            self.stack.push((page, 0));

            self.seek(keyWithoutTxId)?;
        }

        Ok(())
    }

    /// must on leaf
    pub(crate) fn currentKV(&self) -> Option<(&[u8], &[u8])> {
        let (currentPage, currentIndexInPage) = self.stackTop();
        assert!(currentPage.isLeaf());

        if currentPage.elems.is_empty() {
            return None;
        }

        match currentPage.elems.get(*currentIndexInPage).unwrap() {
            PageElem::LeafR(keyWithTxId, val) => Some((keyWithTxId, val)),
            _ => panic!("impossible")
        }
    }
}

// fn
impl<'tx> Cursor<'tx> {
    fn stackTop(&self) -> &(Arc<Page>, usize) {
        self.stack.last().unwrap()
    }

    fn stackTopMut(&mut self) -> &mut (Arc<Page>, usize) {
        self.stack.last_mut().unwrap()
    }
}