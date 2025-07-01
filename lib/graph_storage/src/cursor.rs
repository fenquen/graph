use crate::db::DB;
use crate::page::Page;
use crate::tx::Tx;
use crate::{page, tx};
use anyhow::Result;
use std::sync::{Arc, RwLock};
use crate::page_elem::PageElem;

pub struct Cursor<'tx> {
    db: Arc<DB>,
    tx: Option<&'tx Tx>,

    /// currentPage currentIndexInPage
    stack: Vec<(Arc<RwLock<Page>>, usize)>,

    pub(crate) writeDestLeafPages: Vec<(Arc<RwLock<Page>>, usize)>,
}

// pub fn
impl<'tx> Cursor<'tx> {
    pub fn new(db: Arc<DB>, tx: Option<&'tx Tx>) -> Result<Cursor<'tx>> {
        Ok(Cursor {
            db,
            tx,
            stack: vec![],
            writeDestLeafPages: vec![],
        })
    }
}

// pub (crate) fn
impl<'tx> Cursor<'tx> {
    pub(crate) fn seek(&mut self, key: &[u8], put: bool, val: Option<Vec<u8>>) -> Result<()> {
        self.move2Root()?;
        self.seek0(key, put, val)?;

        Ok(())
    }

    /// must on leaf
    pub(crate) fn currentKV(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let (currentPage, currentIndexInPage) = self.stackTop();

        let currentPage = currentPage.read().unwrap();
        assert!(currentPage.isLeaf());

        if currentPage.pageElems.is_empty() {
            return None;
        }

        match currentPage.pageElems.get(*currentIndexInPage).unwrap() {
            PageElem::LeafR(keyWithTxId, val) => Some((keyWithTxId.to_vec(), val.to_vec())),
            _ => panic!("impossible")
        }
    }
}

// fn
impl<'tx> Cursor<'tx> {
    fn move2Root(&mut self) -> Result<()> {
        let dbHeader = self.db.getHeader();
        let rootPage = self.db.getPageById(dbHeader.rootPageId, None)?;

        self.stack.clear();
        self.stack.push((rootPage, 0));

        Ok(())
    }

    /// 当insert时候会将元素临时的放到node上 先不着急分裂的
    fn seek0(&mut self, key: &[u8], put: bool, val: Option<Vec<u8>>) -> Result<()> {
        let db = self.db.clone();

        let key0 = match self.tx {
            Some(tx) => tx::appendKeyWithTxId(key, tx.id),
            None => key.to_vec(),
        };

        if put {
            let overflowThreshold = {
                let pageSize = self.db.getHeaderMut().pageSize;
                pageSize as usize / page::OVERFLOW_DIVIDE
            };

            let (currentPage, currentIndexInPage) = self.stack.last_mut().unwrap();

            // 需要clone来打断和上边的self.stack的mut引用的关联,惠及下边的currentPageWriteGuard
            let currentPage = currentPage.clone();
            let mut currentPageWriteGuard = currentPage.write().unwrap();

            // put 当前是leaf的
            if currentPageWriteGuard.header.isLeaf() || currentPageWriteGuard.header.isLeafOverflow() {
                let index =
                    currentPageWriteGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::LeafR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            PageElem::Dummy4PutLeaf(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0),
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            PageElem::Dummy4PutLeafOverflow(keyWithTxIdInElem, _, _) => keyWithTxIdInElem.cmp(&key0),
                            _ => panic!("impossible")
                        }
                    }).map(|index| { // Ok 体系 说明要变动(update/delete)现有
                        if index == 0 { // Ok(0)
                            0
                        } else { // Ok(相同的key的index)
                            // index - 1 // 既然是要变动现有的,是不是应该保留原样
                            index
                        }
                    }).map_err(|index| { // Err 体系 说明是要insert了 ,val是None(想要删掉)的话没有意义不用考虑的
                        if index == 0 { // Err(0)
                            0 // insert 到 头部
                        } else { // Err(比它大的最小的元素的index)
                            // index - 1 // insert 到 前边的1个位置
                            index
                        }
                    });

                match index {
                    Ok(index) => { // Ok说明key是有相同的存在的,要变动(update/delete)现有
                        match val {
                            Some(val) => {  // update
                                currentPageWriteGuard.pageElems[index] = {
                                    if val.len() >= overflowThreshold {
                                        // pos的位置暂时先写0后边统1应对
                                        PageElem::Dummy4PutLeafOverflow(key0, 0, val)
                                    } else {
                                        PageElem::Dummy4PutLeaf(key0, val)
                                    }
                                };

                                *currentIndexInPage = index;
                            }
                            None => { // delete
                                currentPageWriteGuard.pageElems.remove(index);

                                // removed one is the last, index equals with vec current length
                                if index == currentPageWriteGuard.pageElems.len() {
                                    *currentIndexInPage = currentPageWriteGuard.pageElems.len() - 1
                                }
                            }
                        }
                    }
                    Err(index) => { // new one to insert 需要落地的1个新的page上的
                        if let Some(val) = val {
                            let pageElem = {
                                if val.len() >= overflowThreshold {
                                    // pos的位置暂时先写0后边统1应对
                                    PageElem::Dummy4PutLeafOverflow(key0, 0, val)
                                } else {
                                    PageElem::Dummy4PutLeaf(key0, val)
                                }
                            };

                            // 说明要加入的key是比pageElems所有元素都大,添加到末尾
                            if index >= currentPageWriteGuard.pageElems.len() {
                                currentPageWriteGuard.pageElems.push(pageElem);
                            } else {
                                currentPageWriteGuard.pageElems.insert(index, pageElem);
                            }
                        }

                        *currentIndexInPage = index;
                    }
                }

                // 收集全部收到影响的leaf page
                self.writeDestLeafPages.push((currentPage.clone(), *currentIndexInPage));
            } else { // put当前是branch的
                let indexResult =
                    currentPageWriteGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::BranchR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            _ => panic!("impossible")
                        }
                    });

                // 
                let index = match indexResult {
                    // 要insert的key大于当前的branch的最大的key的,意味着需要在末尾追加的
                    Err(index) if index >= currentPageWriteGuard.pageElems.len() => {
                        let mut dummyLeafPage = Page::buildDummyLeafPage();
                        dummyLeafPage.parentPage = Some(currentPage.clone());

                        // 添加到末尾
                        currentPageWriteGuard.pageElems.push(PageElem::Dummy4PutBranch(key0, Arc::new(RwLock::new(dummyLeafPage))));

                        *currentIndexInPage = index;

                        index
                    }
                    // use current existing branch page element 说明是相同的契合直接覆盖的
                    Err(index) | Ok(index) => {
                        *currentIndexInPage = index;

                        index
                    }
                };

                currentPageWriteGuard.indexInParentPage = Some(index);

                // 最后时候 就当前的情况添加内容到stack的
                match currentPageWriteGuard.pageElems.get(index).unwrap() {
                    PageElem::BranchR(_, pageId) => {
                        let page = db.getPageById(*pageId, Some(currentPage.clone()))?;
                        self.stack.push((page, 0));
                    }
                    PageElem::Dummy4PutBranch(_, page) => {
                        self.stack.push((page.clone(), 0));
                    }
                    _ => panic!("impossible")
                }

                self.seek0(key, put, val)?;
            }
        } else { // 不是put
            let (currentPageArc, currentIndexInPage) = self.stackTopMut();
            let currentPageReadGuard = currentPageArc.read().unwrap();

            // leaf
            // try to locate the index in page
            if currentPageReadGuard.header.isLeaf() || currentPageReadGuard.header.isLeafOverflow() {
                // returns the the index of minimal value which is greater or equal with the search value
                // if there is an equivalent value ,then returns Ok(index) else returns Err(index)
                let index =
                    currentPageReadGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::LeafR(keyWithTxIdInElem, _) |
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            _ => panic!("impossible")
                        }
                    }).unwrap_or_else(|index| {
                        // means that there is no equivalent key
                        // the index represents the minimal key larger than target key
                        if index == 0 {
                            0
                        } else {
                            index - 1
                        }
                    });

                *currentIndexInPage = index;
            } else { // branch
                let index =
                    currentPageReadGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::BranchR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            _ => panic!("impossible")
                        }
                    }).unwrap_or_else(|index| {
                        // none, the target has no possibility in the descendant
                        // even thought ,we still need to go on
                        if index >= currentPageReadGuard.pageElems.len() {
                            index - 1
                        } else {
                            index
                        }
                    });

                *currentIndexInPage = index;

                let pageId = {
                    let pageElem = currentPageReadGuard.pageElems.get(index).unwrap();
                    let (_, pageId) = pageElem.asBranchR()?;
                    pageId
                };

                drop(currentPageReadGuard);

                let page = db.getPageById(pageId, Some(currentPageArc.clone()))?;
                self.stack.push((page, 0));

                self.seek0(key, put, val)?;
            }
        }

        Ok(())
    }

    fn stackTop(&self) -> &(Arc<RwLock<Page>>, usize) {
        self.stack.last().unwrap()
    }

    fn stackTopMut(&mut self) -> &mut (Arc<RwLock<Page>>, usize) {
        self.stack.last_mut().unwrap()
    }
}