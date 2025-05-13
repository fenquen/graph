use std::hint::unlikely;
use std::ops::DerefMut;
use std::slice;
use crate::page::{Page, PageElem};
use crate::tx::Tx;
use crate::{page, page_header, tx};
use anyhow::Result;
use std::sync::{Arc, RwLock};
use memmap2::MmapMut;
use crate::db::DB;

pub struct Cursor<'tx> {
    db: Arc<DB>,
    tx: Option<&'tx Tx>,

    /// currentPage currentIndexInPage
    stack: Vec<(Arc<RwLock<Page>>, usize)>,

    writeDestLeafPages: Vec<Arc<RwLock<Page>>>,
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

        // 如果是put的话还有善后处理涉及到的全部的leaf的
        if put {
            let pageSize = self.db.getHeader().pageSize as usize;

            for writeDestLeafPage in self.writeDestLeafPages.iter() {
                let mut writeDestLeafPage = writeDestLeafPage.write().unwrap();
                let writeDestLeafPage = writeDestLeafPage.deref_mut();

                let pageIsDummy = writeDestLeafPage.isDummy();

                // 读取writeDestLeafPage当前含有的全部pageElem, 遍历它们挨个写入到page
                // 如果是现有的page,那么可以先复用当前已有的这个page
                let mut curPageMmapMut: Option<MmapMut> = None;
                let mut curPosInPage = 0usize;
                let mut firstPage = true;

                for pageElem in writeDestLeafPage.pageElems.iter_mut() {
                    // 说明要新写1个page了
                    if unlikely(curPageMmapMut.is_none()) {
                        // 确定分配1个新的pageMmapMut
                        curPageMmapMut = {
                            // 对第1个要分配的page来说是不是dummy有区别
                            if firstPage && pageIsDummy == false {
                                // 过会用了后不要忘了还回去
                                writeDestLeafPage.mmapMut.take()
                            } else {
                                Some(self.db.allocateNewPage()?)
                            }
                        };

                        // 给pageHeader留下位置
                        curPosInPage = page_header::PAGE_HEADER_SIZE;
                    }

                    firstPage = false;

                    // 实际写入各个pageElem

                    // page剩下空间不够了
                    if curPosInPage + pageElem.diskSize() > pageSize {
                        if pageIsDummy == false {
                            // 归还原来通过take得到的
                            writeDestLeafPage.mmapMut = curPageMmapMut;
                        }

                        curPageMmapMut = None;
                        curPosInPage = 0;

                        continue;
                    }

                    let curPageMmapMut = curPageMmapMut.as_mut().unwrap();

                    // 化为指针
                    let ptr = unsafe { (curPageMmapMut as *mut _ as *mut u8).add(curPosInPage) };

                    let destSlice = unsafe { slice::from_raw_parts_mut(ptr, 1) };

                    pageElem.write2Disk(destSlice);


                    curPosInPage += 1;
                }

                let pageCountNeedAllocate = {
                    let dbHeader = self.db.getHeader();

                    // 这样是用来计算要占用多少个page大小而不是当前是在哪个page
                    // 如果pages是dummy的那么要新分配的数量是totalPageCount
                    // 如果不是的话需要减去原本有的1个,那么是totalPageCount-1
                    let totalPageCount = (writeDestLeafPage.diskSize() + (dbHeader.pageSize as usize - 1)) / dbHeader.pageSize as usize;

                    if writeDestLeafPage.isDummy() {
                        totalPageCount
                    } else {
                        if totalPageCount > 1 {
                            totalPageCount - 1
                        } else {
                            0
                        }
                    }
                };

                // 确实需要额外分配page
                if pageCountNeedAllocate > 0 {
                    for a in writeDestLeafPage.pageElems.iter() {}
                }
            }
        }

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

            let (currentPage, currentIndexInPage) = self.stackTopMut();

            // 需要clone断离和上边的self.stack的mut引用的关联,惠及下边的currentPageWriteGuard
            let currentPage = currentPage.clone();
            let mut currentPageWriteGuard = currentPage.write().unwrap();

            // put 当前是leaf的
            if currentPageWriteGuard.header.isLeaf() || currentPageWriteGuard.header.isLeafOverflow() {
                let index =
                    currentPageWriteGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::LeafR(keyWithTxIdInElem, _) |
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&key0.as_slice()),
                            _ => panic!("impossible")
                        }
                    }).map(|index| { // ok /err value use same process
                        if index == 0 {
                            0
                        } else {
                            index - 1
                        }
                    }).map_err(|index| {
                        if index == 0 {
                            0
                        } else {
                            index - 1
                        }
                    });

                match index {
                    Ok(index) => { // update/delete 落地到当前现有的page
                        match val {
                            // 就地更新替换的
                            Some(val) => {
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
                            // 就地 delete的
                            None => {
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

                            // search 1 from empty slice [] get Err(0)
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
                self.writeDestLeafPages.push(currentPage.clone());
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