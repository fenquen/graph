use std::collections::HashMap;
use crate::db::DB;
use crate::page::Page;
use crate::tx::Tx;
use crate::{page, tx};
use anyhow::Result;
use std::sync::{Arc, RwLock};
use crate::page_elem::PageElem;
use crate::types::{PageId, TxId};

pub struct Cursor<'db, 'tx> {
    db: &'db DB,
    tx: Option<&'tx Tx<'db>>,

    /// currentPage currentIndexInPage
    stack: Vec<(Arc<RwLock<Page>>, usize)>,

    /// pageId -> (page, indexInParent)
    pub(crate) leafPageId2LeafPage: HashMap<PageId, Arc<RwLock<Page>>>,
}

// pub fn
impl<'db, 'tx> Cursor<'db, 'tx> {
    pub fn new(db: &'db DB, tx: Option<&'tx Tx<'db>>) -> Result<Cursor<'db, 'tx>> {
        Ok(Cursor {
            db,
            tx,
            stack: Vec::new(),
            leafPageId2LeafPage: HashMap::new(),
        })
    }

    pub(crate) fn seek(&mut self,
                       key: &[u8],
                       value: Option<&[u8]>,
                       put: bool,
                       txIdThreshold2Delete: TxId) -> Result<()> {
        self.move2Root()?;

        //let mut arr = [0; 8];
        //arr.copy_from_slice(&key[..8]);
        //let a = usize::from_be_bytes(arr);

        let keyWithTxId =
            match self.tx {
                // get读取的时候
                Some(tx) => tx::appendKeyWithTxId(key, tx.id),
                // 写入落地的时候,key本身是含有txId的
                None => key.to_vec(),
            };

        self.seek0(keyWithTxId.as_slice(), value, put, txIdThreshold2Delete)?;

        Ok(())
    }

    /// must on leaf
    pub(crate) fn currentKV(&self) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        let (currentPage, currentIndexInPage) = self.stackTop();

        let currentPage = currentPage.read().unwrap();
        assert!(currentPage.isLeaf());

        if currentPage.pageElems.is_empty() {
            return None;
        }

        match currentPage.pageElems.get(*currentIndexInPage).unwrap() {
            PageElem::LeafR(keyWithTxId, value) =>
                Some((keyWithTxId.to_vec(), value.as_ref().map(|v| v.to_vec()))),
            PageElem::Dummy4PutLeaf(keyWithTxId, value) =>
                Some((keyWithTxId.to_vec(), value.as_ref().map(|v| v.to_vec()))),
            _ => panic!("impossible")
        }
    }

    fn move2Root(&mut self) -> Result<()> {
        let dbHeader = self.db.getHeader();
        let rootPage = self.db.getPageById(dbHeader.rootPageId)?;

        self.stack.clear();
        self.stack.push((rootPage, 0));

        Ok(())
    }

    /// 当insert时候会将元素临时的放到node上 先不着急分裂的
    fn seek0(&mut self,
             targetKeyWithTxId: &[u8],
             value: Option<&[u8]>,
             put: bool,
             txIdThreshold2Delete: TxId) -> Result<()> {
        let db = self.db;

        if put {
            let overflowThreshold = {
                let pageSize = db.getHeaderMut().pageSize;
                pageSize / page::OVERFLOW_DIVIDE
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
                            PageElem::LeafR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&targetKeyWithTxId),
                            PageElem::Dummy4PutLeaf(keyWithTxIdInElem, _) => keyWithTxIdInElem.as_slice().cmp(targetKeyWithTxId),
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&targetKeyWithTxId),
                            PageElem::Dummy4PutLeafOverflow(keyWithTxIdInElem, _, _) => keyWithTxIdInElem.as_slice().cmp(targetKeyWithTxId),
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
                                        PageElem::Dummy4PutLeafOverflow(targetKeyWithTxId.to_vec(), 0, value.to_vec())
                                    } else {
                                        PageElem::Dummy4PutLeaf(targetKeyWithTxId.to_vec(), Some(value.to_vec()))
                                    }
                                };

                                *currentIndexInPage = index;
                            }
                            None => {
                                // delete 这种情况也只有可能是相同的tx内先set然后delete
                                // 不过在changes里就会抵消掉了不会走到这里的
                                currentPageWriteGuard.pageElems.remove(index);

                                // removed one is the last, index equals with vec current length
                                if index == currentPageWriteGuard.pageElems.len() {
                                    *currentIndexInPage = currentPageWriteGuard.pageElems.len() - 1
                                }
                            }
                        }
                    }
                    Err(index) => { // new one to insert 需要落地的1个新的page上的
                        let pageElem =
                            if let Some(value) = value {
                                if value.len() >= overflowThreshold {
                                    // pos的位置暂时先写0后边统1应对
                                    PageElem::Dummy4PutLeafOverflow(targetKeyWithTxId.to_vec(), 0, value.to_vec())
                                } else {
                                    PageElem::Dummy4PutLeaf(targetKeyWithTxId.to_vec(), Some(value.to_vec()))
                                }
                            } else { // 即使是delete,value要是要用none占位
                                PageElem::Dummy4PutLeaf(targetKeyWithTxId.to_vec(), None)
                            };

                        // 说明要加入的key是比pageElems所有元素都大,添加到末尾
                        if index >= currentPageWriteGuard.pageElems.len() {
                            currentPageWriteGuard.pageElems.push(pageElem);
                        } else {
                            currentPageWriteGuard.pageElems.insert(index, pageElem);
                        }

                        *currentIndexInPage = index;
                    }
                }

                // 在当前的leafPage上清洗
                let targetKeyWithoutTxId = tx::getKeyFromKeyWithTxId(targetKeyWithTxId);

                // 是不是需要到prevPage清理
                let mut needGo2PrevPage = true;

                // 倒序
                for index in (0..*currentIndexInPage).rev() {
                    let keyWithTxIdOfElem = &currentPageWriteGuard.pageElems[index].getKey();

                    let (keyWithoutTxIdOfElem, txIdOfElem) = tx::parseKeyWithTxId(keyWithTxIdOfElem);

                    // 在当前的leafPage倒序上清洗,碰到了纯key不同 || txId 不在清理范围了
                    // 说明已经到头了,也不需要到prevPage清理
                    if targetKeyWithoutTxId != keyWithoutTxIdOfElem || txIdOfElem >= txIdThreshold2Delete {
                        needGo2PrevPage = false;
                        break;
                    }

                    // 后续在memTableR的writePages时候,如果page足够空闲了会尝试和其它的合并以节省空间的
                    currentPageWriteGuard.pageElems.remove(index);
                }

                // 当前的leafPage清理掉了,还要不断向前递进清理掉
                if needGo2PrevPage {
                    let mut prevPageId = currentPageWriteGuard.header.prevPageId;

                    'a:
                    loop {
                        if prevPageId == 0 {
                            break;
                        }

                        let prevPage0 = db.getPageById(prevPageId)?;
                        let mut prevPage = prevPage0.write().unwrap();

                        let mut hasElemRemoved = false;

                        for index in (0..prevPage.pageElems.len()).rev() {
                            let keyWithTxIdOfElem = &prevPage.pageElems[index].getKey();

                            let (keyWithoutTxIdOfElem, txIdOfElem) = tx::parseKeyWithTxId(keyWithTxIdOfElem);

                            // 在当前的leafPage倒序上清洗,碰到了纯key不同 || txId 不在清理范围了
                            // 说明已经到头了,也不需要到prevPage清理
                            if targetKeyWithoutTxId != keyWithoutTxIdOfElem || txIdOfElem >= txIdThreshold2Delete {
                                break 'a;
                            }

                            // 不能在这更新pageHeader中的elemCount,因为elemCount是在page的write2Disk()时候明确的,那是在后面调用的
                            // 这个时候elemCount 和 pageElems.len() 是不对应的
                            //let prevPageElemCount = prevPage.pageElems.len();
                            //assert_eq!(prevPage.header.elemCount as usize, prevPageElemCount);
                            //prevPage.header.elemCount -= 1;

                            prevPage.pageElems.remove(index);

                            hasElemRemoved = true;
                        }

                        if hasElemRemoved {
                            // 能够知道这个page发生了改动,以便让后续对它重写,这样也能更新header中的elemCount了
                            self.leafPageId2LeafPage.insert(prevPage.header.id, prevPage0.clone());
                        }

                        prevPageId = prevPage.header.prevPageId;
                    }
                }

                //if currentPageWriteGuard.header.indexInParentPage.is_some() {
                assert_eq!(currentPageWriteGuard.header.indexInParentPage, self.getIndexInParentPage());
                //}

                // 收集全部收到影响的leaf page
                // 需要知道有没有经历过历经branch的下钻过程,如果上手便是身为leaf的root page 这样是不对的
                if self.leafPageId2LeafPage.contains_key(&currentPageId) == false {
                    self.leafPageId2LeafPage.insert(currentPageId, currentPage.clone());
                }
            } else { // put, 当前是branch
                let indexResult =
                    currentPageWriteGuard.pageElems.binary_search_by(|pageElem| {
                        match pageElem {
                            PageElem::BranchR(keyWithTxId, _) => keyWithTxId.cmp(&targetKeyWithTxId),
                            PageElem::Dummy4PutBranch(keyWithTxId, _) => keyWithTxId.as_slice().cmp(targetKeyWithTxId),
                            _ => panic!("impossible")
                        }
                    });

                let index =
                    match indexResult {
                        // 要insert的key大于当前的branch的最大的key的,这里是将它落地到了最后1个元素
                        // 单单这么看的话似乎有问题,按照道理应该是在末尾再添加1个branch elem的
                        // 然而当read的时候碰到这样的情况时候(Err的index >= pageElems长度)也是读取的最后1个元素
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

                //currentPageWriteGuard.indexInParentPage = self.getIndexInParentPage();

                // 最后时候 就当前的情况添加内容到stack的
                match currentPageWriteGuard.pageElems.get(index).unwrap() {
                    PageElem::BranchR(_, pageId) => {
                        let page = db.getPageById(*pageId)?;
                        self.stack.push((page, 0));
                    }
                    PageElem::Dummy4PutBranch(_, pageHeader) => {
                        let childPage = db.getPageById(pageHeader.id)?;
                        self.stack.push((childPage, 0))
                    }
                    _ => panic!("impossible")
                }

                drop(currentPageWriteGuard);

                // key 小于 txIdThreshold2Delete 的 ones 可能分布在多个leaf中
                self.seek0(targetKeyWithTxId, value, put, txIdThreshold2Delete)?;
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
                        match pageElem {
                            PageElem::LeafR(keyWithTxIdInElem, _) |
                            PageElem::LeafOverflowR(keyWithTxIdInElem, _) => keyWithTxIdInElem.cmp(&targetKeyWithTxId),
                            PageElem::Dummy4PutLeaf(keyWithTxIdInElem, _) |
                            PageElem::Dummy4PutLeafOverflow(keyWithTxIdInElem, _, _) => keyWithTxIdInElem.as_slice().cmp(targetKeyWithTxId),
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
                        match pageElem {
                            PageElem::BranchR(keyWithTxIdInElem, _) => (*keyWithTxIdInElem).cmp(&targetKeyWithTxId),
                            PageElem::Dummy4PutBranch(keyWithTxIdInElem, _) => keyWithTxIdInElem.as_slice().cmp(targetKeyWithTxId),
                            _ => panic!("impossible")
                        }
                    }).unwrap_or_else(|index| {
                        // Err说明不存在相等的key
                        // index是大于它的最小元素的index
                        // 特殊情况 数组是空的返回Err(0); 数组元素都要比它小返回Err(数组长度)
                        //
                        // 以下的逻辑的原因:
                        // 假设branchElem的key是这些: 250+txId:1 270+txId:2
                        // 当前要搜索的key是250+txId:7,如果以普通逻辑那么会选中270+txId:2 即大于它的最小元素
                        // 这样是不对的,显然要找的value是在250+txId:1那里
                        // 250+txId:1和270+txId:2 分别是小于它的最大元素和大于它的最小元素,它们是相邻的
                        // 在代码中需要向前瞧瞧index-1那的不含txId的key是否和当前的keyWithoutTxId相同
                        // 根本的原因是250+txId:1成为了branchElem的key且和需要搜索的相同
                        // 如果要搜索的key的纯粹部分未出现在branchElem的key中,例如要搜索的是251+tx:7
                        // 那么普通逻辑返回的大于它的最小值270+txId:2是正确的的
                        if index > 0 {
                            // index -1 是 小于自己的最大元素的index
                            let prevPageElem = currentPageReadGuard.pageElems.get(index - 1).unwrap();

                            let prevKeyWithoutTxId = tx::getKeyFromKeyWithTxId(prevPageElem.getKey());
                            let key0WithoutTxId = tx::getKeyFromKeyWithTxId(&targetKeyWithTxId);

                            // 前边(小于自己的最大元素)的key不含txId和自己不含txId相同
                            // 同时意味着prevKey的txId小于key0的txId
                            if prevKeyWithoutTxId == key0WithoutTxId {
                                index - 1
                            } else {
                                // 数组元素都要比它小返回Err(数组长度)
                                if index >= currentPageReadGuard.pageElems.len() {
                                    index - 1
                                } else {
                                    index
                                }
                            }
                        } else { // 数组是空的
                            0
                        }
                    });

                *currentIndexInPage = index;

                let pageId = {
                    let pageElem = match currentPageReadGuard.pageElems.get(index) {
                        Some(pageElem) => pageElem,
                        None => panic!("impossible")
                    };

                    match pageElem {
                        PageElem::BranchR(_, pageId) => *pageId,
                        PageElem::Dummy4PutBranch(_, pageHeader) => pageHeader.id,
                        _ => panic!("impossible")
                    }
                };

                drop(currentPageReadGuard);

                let page = db.getPageById(pageId)?;
                self.stack.push((page, 0));

                self.seek0(targetKeyWithTxId, value, put, txIdThreshold2Delete)?;
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