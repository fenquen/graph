use std::collections::HashMap;
use crate::db::DB;
use crate::page::Page;
use crate::tx::Tx;
use crate::{page, tx};
use anyhow::Result;
use std::sync::{Arc, RwLock};
use crate::page_elem::PageElem;
use crate::types::PageId;

pub struct Cursor<'db, 'tx> {
    db: &'db DB,
    tx: Option<&'tx Tx<'db>>,

    /// currentPage currentIndexInPage
    stack: Vec<(Arc<RwLock<Page>>, usize)>,

    /// pageId -> (page, indexInParent)
    pub(crate) pageId2PageAndIndexInParent: HashMap<PageId, (Arc<RwLock<Page>>, Option<usize>)>,
}

// pub fn
impl<'db, 'tx> Cursor<'db, 'tx> {
    pub fn new(db: &'db DB, tx: Option<&'tx Tx<'db>>) -> Result<Cursor<'db, 'tx>> {
        Ok(Cursor {
            db,
            tx,
            stack: Vec::new(),
            pageId2PageAndIndexInParent: HashMap::new(),
        })
    }

    pub(crate) fn seek(&mut self, key: &[u8], put: bool, value: Option<&[u8]>) -> Result<()> {
        self.move2Root()?;

        //let mut arr = [0; 8];
        //arr.copy_from_slice(&key[..8]);
        //let a = usize::from_be_bytes(arr);

        self.seek0(key, put, value)?;

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
            PageElem::Dummy4PutLeaf(keyWithTxId, val) => Some((keyWithTxId.to_vec(), val.to_vec())),
            _ => panic!("impossible")
        }
    }

    fn move2Root(&mut self) -> Result<()> {
        let dbHeader = self.db.getHeader();
        let rootPage = self.db.getPageById(dbHeader.rootPageId, None)?;

        self.stack.clear();
        self.stack.push((rootPage, 0));

        Ok(())
    }

    /// 当insert时候会将元素临时的放到node上 先不着急分裂的
    fn seek0(&mut self, key: &[u8], put: bool, value: Option<&[u8]>) -> Result<()> {
        let db = self.db;

        let key0 = match self.tx {
            // get读取的时候
            Some(tx) => tx::appendKeyWithTxId(key, tx.id),
            // 写入落地的时候,key本身是含有txId的
            None => key.to_vec(),
        };

        if put {
            let overflowThreshold = {
                let pageSize = db.getHeaderMut().pageSize;
                pageSize as usize / page::OVERFLOW_DIVIDE
            };

            let (currentPage, currentIndexInPage) = self.stack.last_mut().unwrap();

            // 需要clone来打断和上边的self.stack的mut引用的关联,惠及下边的currentPageWriteGuard
            let currentPage = currentPage.clone();
            let mut currentPageWriteGuard = currentPage.write().unwrap();

            let currentPageId = currentPageWriteGuard.header.id;

            // put, 当前是leaf的
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
                        match value {
                            Some(value) => {  // update
                                currentPageWriteGuard.pageElems[index] = {
                                    if value.len() >= overflowThreshold {
                                        // pos的位置暂时先写0后边统1应对
                                        PageElem::Dummy4PutLeafOverflow(key0, 0, value.to_vec())
                                    } else {
                                        PageElem::Dummy4PutLeaf(key0, value.to_vec())
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
                        if let Some(value) = value {
                            let pageElem = {
                                if value.len() >= overflowThreshold {
                                    // pos的位置暂时先写0后边统1应对
                                    PageElem::Dummy4PutLeafOverflow(key0, 0, value.to_vec())
                                } else {
                                    PageElem::Dummy4PutLeaf(key0, value.to_vec())
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
                // 需要知道有没有经历过历经branch的下钻过程,如果上手便是身为leaf的root page 这样是不对的
                if self.pageId2PageAndIndexInParent.contains_key(&currentPageId) == false {
                    self.pageId2PageAndIndexInParent.insert(currentPageId, (currentPage.clone(), self.getIndexInParentPage()));
                }
            } else { // put, 当前是branch
                let indexResult =
                    currentPageWriteGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::BranchR(keyWithTxId, _) => keyWithTxId.cmp(&key0.as_slice()),
                            PageElem::Dummy4PutBranch(keyWithTxId, _) |
                            PageElem::Dummy4PutBranch0(keyWithTxId, _) => keyWithTxId.as_slice().cmp(key0.as_slice()),
                            _ => panic!("impossible")
                        }
                    });

                // 
                let index = match indexResult {
                    // 要insert的key大于当前的branch的最大的key的,意味着需要在末尾追加的
                    Err(index) if index >= currentPageWriteGuard.pageElems.len() => {
                        *currentIndexInPage = index - 1;
                        index - 1
                    }
                    // use current existing branch page element 说明是相同的契合直接覆盖的
                    Err(index) | Ok(index) => {
                        *currentIndexInPage = index;
                        index
                    }
                };

                currentPageWriteGuard.indexInParentPage = self.getIndexInParentPage();

                // 最后时候 就当前的情况添加内容到stack的
                match currentPageWriteGuard.pageElems.get(index).unwrap() {
                    PageElem::BranchR(_, pageId) | PageElem::Dummy4PutBranch0(_, pageId) => {
                        let page = db.getPageById(*pageId, Some(currentPage.clone()))?;
                        self.stack.push((page, 0));
                    }
                    PageElem::Dummy4PutBranch(_, page) => {
                        self.stack.push((page.clone(), 0));
                    }
                    _ => panic!("impossible")
                }

                drop(currentPageWriteGuard);

                self.seek0(key, put, value)?;
            }
        } else { // 不是put
            let (currentPage, currentIndexInPage) = self.stackTopMut();
            let currentPageReadGuard = currentPage.read().unwrap();

            // leaf
            // try to locate the index in page
            if currentPageReadGuard.header.isLeaf() || currentPageReadGuard.header.isLeafOverflow() {
                // returns the the index of minimal value which is greater or equal with the search value
                // if there is an equivalent value ,then returns Ok(index) else returns Err(index)
                let index =
                    currentPageReadGuard.pageElems.binary_search_by(|pageElem| {
                        let key0 = key0.as_slice();

                        match pageElem {
                            PageElem::LeafR(keyWithTxIdInElem, _) |
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0),
                            PageElem::Dummy4PutLeaf(keyWithTxIdInElem, _) |
                            PageElem::Dummy4PutLeafOverflow(keyWithTxIdInElem, _, _) => keyWithTxIdInElem.as_slice().cmp(key0),

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
                // 读branch的时候key不应还有tx的
                let index =
                    currentPageReadGuard.pageElems.binary_search_by(|pageElem| {
                        let key0WithoutTxId = tx::extractKeyFromKeyWithTxId(&key0);

                        match pageElem {
                            // branch elem中保存的key 要不要含有txId
                            // 至少在gets时候在branch上的筛选比较key是不能含有txId的
                            // 碰到某种情况,branchPage中的某个elem的key是250,txId是1
                            // 然后在txId是2的时候尝试get 250对应的val
                            // 如果将txId考虑在内进行key的比较的话会得不到value,这是不对的
                            PageElem::BranchR(keyWithTxIdInElem, _) => {
                                let keyInElem = tx::extractKeyFromKeyWithTxId(keyWithTxIdInElem);
                                keyInElem.cmp(key0WithoutTxId)
                            }
                            PageElem::Dummy4PutBranch(keyWithTxIdInElem, _) |
                            PageElem::Dummy4PutBranch0(keyWithTxIdInElem, _) => {
                                let keyInElem = tx::extractKeyFromKeyWithTxId(keyWithTxIdInElem);
                                keyInElem.cmp(key0WithoutTxId)
                            }
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

                    match pageElem {
                        PageElem::BranchR(_, pageId) => *pageId,
                        PageElem::Dummy4PutBranch(_, page) => {
                            let page = page.read().unwrap();
                            page.header.id
                        }
                        PageElem::Dummy4PutBranch0(_, pageId) => *pageId,
                        _ => panic!("impossible")
                    }
                };

                drop(currentPageReadGuard);

                let page = db.getPageById(pageId, Some(currentPage.clone()))?;
                self.stack.push((page, 0));

                self.seek0(key, put, value)?;
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

    fn getIndexInParentPage(&self) -> Option<usize> {
        if self.stack.len() >= 2 {
            self.stack.get(self.stack.len() - 2).map(|(_, indexInPage)| *indexInPage)
        } else {
            None
        }
    }
}