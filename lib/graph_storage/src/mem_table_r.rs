/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use std::collections::HashMap;
use std::{mem, ptr};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::DerefMut;
use std::os::fd::RawFd;
use std::sync::{Arc, RwLock};
use memmap2::{Advice, MmapMut};
use crate::mem_table::{MemTableFileHeader};
use crate::{mem_table, page_header, tx, utils};
use anyhow::Result;
use crate::cursor::Cursor;
use crate::db::DB;
use crate::page::Page;
use crate::page_elem::PageElem;
use crate::page_header::PageHeader;
use crate::types::PageId;

pub(crate) struct MemTableR {
    memTableFileMmap: MmapMut,
}

impl TryFrom<RawFd> for MemTableR {
    type Error = anyhow::Error;

    fn try_from(fd: RawFd) -> Result<Self, Self::Error> {
        let memTableFileMmap = {
            let memTableFileMmap = utils::mmapFdMut(fd, None, None)?;
            memTableFileMmap.advise(Advice::WillNeed)?;
            memTableFileMmap
        };

        Ok(MemTableR {
            memTableFileMmap,
        })
    }
}

impl MemTableR {
    pub(crate) fn iter(&self) -> MemTableRIter<'_> {
        let fileHeader: &MemTableFileHeader = utils::slice2Ref(&self.memTableFileMmap);

        MemTableRIter {
            data: &self.memTableFileMmap,
            entryCountRemain: fileHeader.entryCount as usize,
            posInfile: mem_table::MEM_TABLE_FILE_HEADER_SIZE,
        }
    }
}

/// 要同时有entryCount和posFile的原因是memTableFile末尾可能是有空白的未全部写满的
pub(crate) struct MemTableRIter<'a> {
    data: &'a [u8],
    entryCountRemain: usize,
    posInfile: usize,
}

impl<'a> Iterator for MemTableRIter<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.entryCountRemain == 0 {
            return None;
        }

        let (key, value, entrySize) =
            mem_table::readEntry(&self.data[self.posInfile..]);

        self.entryCountRemain -= 1;
        self.posInfile += entrySize;

        Some((key, value))
    }
}

// 当写完了1个memTableR如何通知对应的immutableTable清理
// 目前的话不能精确到通知单个immutable只能是以batch维度,因为memTableR的内容都融合到1起了
pub(crate) fn processMemTableRs(db: &DB, memTableRs: Vec<MemTableR>) -> Result<()> {
    let flyingTxIdMinimal = {
        let flyingTxIds = db.flyingTxIds.read().unwrap();
        flyingTxIds.first().copied()
    };

    let mut cursor = Cursor::new(db, None)?;

    // 如果某个key对应的txId "a" 不在infightingTxIds中 且 对应的value是None
    // 那么这个key的全部小于等于a体系的都可以干掉了
    for memTableR in memTableRs.iter() {
        for (key, value) in memTableR.iter() {
            let txIdThreshold2Delete =
                if flyingTxIdMinimal.is_none() {
                    let (_, txId) = tx::parseKeyWithTxId(key);
                    txId
                } else {
                    flyingTxIdMinimal.unwrap()
                };

            // 试图删掉小于txIdThreshold的全部版本
            cursor.seek(key, value, true, txIdThreshold2Delete)?;
        }
    }

    let mut involvedParentPages = HashMap::new();

    // 这里的都是leafPage
    writePages(db, cursor.leafPageId2LeafPage.into_values().collect(), &mut involvedParentPages)?;

    loop {
        let involvedParentPagesPrevRound: Vec<Arc<RwLock<Page>>> = {
            let pairs: Vec<(PageId, Arc<RwLock<Page>>)> = involvedParentPages.drain().collect();
            pairs.into_iter().map(|(_, involvedParentPage)| involvedParentPage).collect()
        };

        if involvedParentPagesPrevRound.is_empty() {
            break;
        }

        let page2IndexInParentPage =
            involvedParentPagesPrevRound.into_iter().map(|involvedParentPage| {
                /* let indexInParentPage = {
                     let pageReadGuard = involvedParentPage.read().unwrap();
                     pageReadGuard.indexInParentPage
                 };*/

                // (involvedParentPage, indexInParentPage)

                involvedParentPage
            }).collect::<Vec<_>>();

        writePages(db, page2IndexInParentPage, &mut involvedParentPages)?;
    }

    // page的allocator pattern 持久化
    {
        let mut pageAllocator = db.pageAllocator.write().unwrap();
        pageAllocator.refresh();
    }

    // memTableRs中的元素已经全部落地了,通知对应的immutable memTable
    for memTableR in memTableRs.iter() {
        let header: &mut MemTableFileHeader = utils::slice2RefMut(&memTableR.memTableFileMmap);
        header.written2Disk = true
    }

    Ok(())
}

fn writePages(db: &DB, writeDestPages: Vec<Arc<RwLock<Page>>>,
              parentPagesNeedRewrite: &mut HashMap<PageId, Arc<RwLock<Page>>>) -> Result<()> {

    // 用来收集因为空间利用率太低合并受影响的page
    let mut pagesInfluencedByMerge = Vec::<_>::new();

    for writeDestPageArc in writeDestPages {
        let mut writeDestPageGuard = writeDestPageArc.write().unwrap();
        let mut writeDestPage = writeDestPageGuard.deref_mut();

        let pageIdUnderMutex = writeDestPage.header.id;

        if writeDestPage.header.indexInParentPage.is_none() {
            assert_eq!(db.getHeader().rootPageId, writeDestPage.header.id);
            assert_eq!(writeDestPage.header.parentPageId, 0);
        } else {
            assert_ne!(writeDestPage.header.parentPageId, 0);
        }

        let mut replacement = MaybeUninit::uninit();

        // 目前已知的会这样的情况是 cursor时候的顺便删掉旧版本、下边的当page过度空闲的合并的
        if writeDestPage.pageElems.is_empty() {
            match writeDestPage.header.indexInParentPage {
                // 说明是rootPage了,它就算空了也是要保持住的
                None => {}
                Some(indexInParentPage) => {
                    let parentPage0 = db.getPageById(writeDestPage.header.parentPageId)?;
                    let mut parentPage = parentPage0.write().unwrap();

                    parentPagesNeedRewrite.insert(writeDestPage.header.parentPageId, parentPage0.clone());

                    // 它原来在parentPage的栏位要干掉
                    parentPage.pageElems.remove(indexInParentPage);
                }
            }

            continue;
        }

        // 如果page会分裂成多个的话,是会受到填充率的限制的
        writeDestPage.write2Disk()?;

        // 说明在write2Disk时候因为元素太多又另外分配了新的page,原来的self对应的page后续要回收了
        if let Some(page) = writeDestPage.replacement.take() {
            replacement.write(unsafe {
                let pointer = Box::into_raw(page);
                // 浅拷贝MauallyDrop<Page>
                let manuallyDrop = ptr::read(pointer);
                let replacement = ManuallyDrop::into_inner(manuallyDrop);

                // 利用box本身的析构 清理掉ManuallyDrop<Page>本身在heap上占用的空间,同时不去清理page
                drop(Box::from_raw(pointer));

                replacement
            });

            //原来的writeDestPage不能在这个时候注销掉
            //db.free(writeDestPage);

            writeDestPage = unsafe { replacement.assume_init_mut() };

            // 原来id对应的lock释放
            // drop(writeDestPageGuard);
        }

        // 原来的单个的leaf 现在成了多个 需要原来的那个单个的leaf的parantPage来应对
        // 得要知道当前的这个page在父级的那个位置,然后在对应的位置塞入data
        // 例如 原来 这个leafPage在它的上级中是对应(700,750]的
        // 现在的话(700,750]这段区间又要分裂了
        // 原来是单单这1个区间对应1个leaf,现在是分成多个各自对应单个leaf的

        // 现在要知道各个分裂出来的leafPage的最大的key是多少
        // 要现在最底下的writeDestLeafPage层上平坦横向scan掉然后再到上级的

        // 读取当前的additionalPage的最大的key
        // 是否有必要通过读取mmap来知道,能不能通过直接通过pageElems属性得到啊

        // 当前的page塞不下了,实际要分裂成多个了,需要再在上头盖1个branch
        // 如果说rootPage还是容纳的下 那么不用去理会了
        let needBuildNewParentPage =
            if writeDestPage.additionalPages.len() > 0 {
                writeDestPage.header.parentPageId == 0
            } else {
                false
            };

        // 说明要分裂为多个 且没有parentPage,那它就是rootPage了
        if needBuildNewParentPage {
            assert!(writeDestPage.header.indexInParentPage.is_none())
        }

        let parentPageId =
            // 到了顶头没有上级了
            if needBuildNewParentPage {
                //db.allocateNewPage(page_header::PAGE_FLAG_BRANCH)?;
                let newParentPage = {
                    // 会有这样的情况 起始的时候就1个leafPage 它的id是1
                    let mut allocatedPages = db.allocatePagesByCount(1, db.getHeader().pageSize, page_header::PAGE_FLAG_BRANCH)?;
                    allocatedPages.remove(0)
                };

                let newParentPageId = newParentPage.header.id;

                // 因为rootPage变动了,dbHeader的rootPageId也要相应的变化
                db.getHeaderMut().rootPageId = newParentPageId;

                writeDestPage.header.parentPageId = newParentPageId;

                newParentPageId
            } else {
                // 说明writeDestPage本身是那个的rootPage
                if writeDestPage.header.parentPageId == 0 {
                    continue;
                } else {
                    writeDestPage.header.parentPageId
                }
            };

        let parentPage = db.getPageById(writeDestPage.header.parentPageId)?; //writeDestPage.parentPage.as_ref().unwrap();

        // 添加之前先瞧瞧是不是已经有了相应的pageId了
        if parentPagesNeedRewrite.contains_key(&parentPageId) == false {
            parentPagesNeedRewrite.insert(parentPageId, parentPage.clone());
        }

        // todo 需要应对新的问题pageId可能是之前循环过的lrucache中还有导致lock的重入的
        let mut parentPageWriteGuard = parentPage.write().unwrap();

        // writeDestPage本身对应的branch elem在branch page的index的
        let writeDestPageIndexInParentPage = {
            let lastKeyInPage = writeDestPage.getLastKey();

            let writeDestPageHeader = *utils::slice2RefMut(&writeDestPage.mmapMut);

            let writeDestPageIndexInParentPage =
                match writeDestPage.header.indexInParentPage {
                    Some(indexInParentPage) => {
                        // 这是不可能的,因为indexInParentPage不是none意味着已有了parentPage,不可能是needBuildNewParentPage
                        if needBuildNewParentPage {
                            parentPageWriteGuard.pageElems.push(PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPageHeader));
                            0
                        } else {
                            parentPageWriteGuard.pageElems[indexInParentPage] = PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPageHeader);
                            indexInParentPage
                        }
                    }
                    None => { // 说明直接到了leafPage未经过branch 要么 这是1个临时新建的branch
                        parentPageWriteGuard.pageElems.push(PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPageHeader));
                        0
                    }
                };

            writeDestPage.header.indexInParentPage = Some(writeDestPageIndexInParentPage);

            writeDestPageIndexInParentPage
        };

        // 如果还有additionalPage的话,就要在indexInParentPage后边不断的塞入的
        // 使用drain是因为原来的测试是put之后重启在测试get,如果不重启直接get需要这样干清理掉的
        let mut additionalPages = writeDestPage.additionalPages.drain(..).collect::<Vec<_>>();

        // writeDestPage体系内的pages(writeDestPage自身 加上 additionalPages)实现首尾相连
        for (index, additionalPage) in additionalPages.iter().enumerate() {
            // 要这样的原因是 additionalPage.header.prevPageId = writeDestPage.header.id; 会报错 can not assign which behind "&" reference
            let additionalPageHeader: &mut PageHeader = utils::slice2RefMut(&additionalPage.mmapMut);

            // writeDestPage体系内的pages(writeDestPage自身 加上 additionalPages) 首尾相连
            if index == 0 { // 第1个additionalPage,它和writeDestPage关联
                writeDestPage.header.nextPageId = additionalPageHeader.id;
                additionalPageHeader.prevPageId = writeDestPage.header.id;
            } else { // 前后additionPage相互关联
                let prevAdditionalPage = &additionalPages[index - 1];
                let prevAdditionalPageHeader: &mut PageHeader = utils::slice2RefMut(&prevAdditionalPage.mmapMut);

                prevAdditionalPageHeader.nextPageId = additionalPageHeader.id;
                additionalPageHeader.prevPageId = prevAdditionalPageHeader.id;
            }

            // 顺便设置它们的parentPageId
            additionalPageHeader.parentPageId = parentPageId;
        }

        writeDestPage.header.parentPageId = parentPageId;

        // writeDestPage体系内的pages 和外部的前后首尾相连
        {
            // parentPage.pageElems[writeDestPageIndexInParentPage-1] 和 它们中的第1个(writeDestPage) 关联
            if writeDestPageIndexInParentPage >= 1 {
                let pageElem = &mut parentPageWriteGuard.pageElems[writeDestPageIndexInParentPage - 1];

                match pageElem {
                    PageElem::BranchR(_, pageId) => {
                        let page = db.getPageById(*pageId)?;
                        let mut page = page.write().unwrap();

                        page.header.nextPageId = writeDestPage.header.id;
                        writeDestPage.header.prevPageId = page.header.id;
                    }
                    PageElem::Dummy4PutBranch(_, pageHeader) => {
                        pageHeader.nextPageId = writeDestPage.header.id;
                        writeDestPage.header.prevPageId = pageHeader.id;
                    }
                    _ => panic!("impossible"),
                }
            }

            // 它们中的最后1个 和 parentPage.pageElems[writeDestPageIndexInParentPage+1] 关联
            if let Some(pageElem) = parentPageWriteGuard.pageElems.get_mut(writeDestPageIndexInParentPage + 1) {
                // 如果additionalPages没有的话lastOne便是writeDestPage本身
                let lastOne =
                    if additionalPages.is_empty() {
                        // 不能这样直接写,因为rust编译器是保守的
                        // 它的视角来看不管additionalPages.is_empty()条件是不是成立,writeDestPage这个&mut都是move到了lastOne
                        // writeDestPage

                        // 只能这样通过野路子了
                        unsafe { mem::transmute(writeDestPage as *mut Page) }
                    } else {
                        additionalPages.last_mut().unwrap()
                    };

                match pageElem {
                    PageElem::BranchR(_, pageId) => {
                        let page = db.getPageById(*pageId)?;
                        let mut page = page.write().unwrap();

                        lastOne.header.nextPageId = page.header.id;
                        page.header.prevPageId = lastOne.header.id;
                    }
                    PageElem::Dummy4PutBranch(_, pageHeader) => {
                        lastOne.header.nextPageId = pageHeader.id;
                        pageHeader.prevPageId = lastOne.header.id;
                    }
                    _ => panic!("impossible"),
                }
            }
        }

        // todo pageElem当remove后page不是饱满的,是不是可以相邻的合并的
        // 如果两个page合并，合并后的占用率是不是应该小于那个填充率的
        // additionalPages空的时候,说明page实际的内容大小是1个page容的下
        // 占用率要比availablePageSize的50%小
        if additionalPages.is_empty() { // 意味着未发生过顶替的,replacement是none
            let mut nextPageId = writeDestPage.header.nextPageId;

            // 说明是有连在后边的page的
            if nextPageId != 0 {
                //let mut currentPage = NonNull::new(writeDestPage as *mut Page).unwrap();
                let mut pageSizeAccumulated = writeDestPage.diskSize();

                pagesInfluencedByMerge.push(writeDestPageArc.clone());

                // todo 合并page时候也要调用pageAllocator的free回收
                loop {
                    let nextPageArc = db.getPageById(nextPageId)?;
                    let mut nextPage = nextPageArc.write().unwrap();

                    let nextPagePayloadSize = nextPage.payloadDiskSize();

                    if pageSizeAccumulated + nextPagePayloadSize > db.availablePageSizeAfterSplit {
                        break;
                    }

                    // next page 上的elem全部迁移到writeDestPage上
                    for pageElemInNextPage in nextPage.pageElems.drain(..) {
                        writeDestPage.pageElems.push(pageElemInNextPage);
                    }

                    pagesInfluencedByMerge.push(nextPageArc.clone());

                    // 两边的elemCount 更改
                    // 这2个page都需要write 如下可以通过page的write2Disk()实现
                    // writeDestPage.header.elemCount += nextPage.header.elemCount;
                    // nextPage.header.elemCount = 0;

                    // nextPageId更改
                    //writeDestPage.header.nextPageId = nextPage.header.id;

                    // next page 对应的栏位要注销掉

                    pageSizeAccumulated += nextPagePayloadSize;

                    match nextPage.header.nextPageId {
                        0 => break,
                        other => nextPageId = other,
                    }
                }
            }
        } else {
            // 虽然调用了两遍for循环有点low的,然后没有别的好套路
            // 要到了最后再去move掉additionalPages落地到parentPage的pageElems了
            let mut additionalPageIndexInParentPage = writeDestPageIndexInParentPage;
            for additionalPage in additionalPages {
                additionalPageIndexInParentPage += 1;

                let additionalPageHeader: &mut PageHeader = utils::slice2RefMut(&additionalPage.mmapMut);
                additionalPageHeader.indexInParentPage = Some(additionalPageIndexInParentPage);

                // 不要忘了additionalPage
                additionalPage.msync()?;

                let lastKeyInPage = additionalPage.getLastKey();

                // insert的目标index可以和len相同,塞到最后的和push相同
                parentPageWriteGuard.pageElems.insert(
                    additionalPageIndexInParentPage,
                    PageElem::Dummy4PutBranch(lastKeyInPage, *additionalPageHeader),
                );
            }
        }

        // todo 完成 page的msync不应该在write2Disk函数里调用应该在外边
        writeDestPage.msync()?;
    }

    // 需要merge的page数量应该至少有2个
    if pagesInfluencedByMerge.len() >= 2 {
        writePages(db, pagesInfluencedByMerge, parentPagesNeedRewrite)?;
    }

    Ok(())
}