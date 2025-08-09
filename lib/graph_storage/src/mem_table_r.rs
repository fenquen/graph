/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use std::collections::HashMap;
use std::ops::DerefMut;
use std::os::fd::RawFd;
use std::sync::{Arc, RwLock};
use memmap2::{Advice, MmapMut};
use crate::mem_table::{MemTable, MemTableFileHeader};
use crate::{mem_table, page_header, utils};
use anyhow::Result;
use crate::cursor::Cursor;
use crate::db::DB;
use crate::page::Page;
use crate::page_elem::PageElem;
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
    let mut cursor = Cursor::new(db, None)?;

    // 如果某个key对应的txId a 不在infightingTxIds中 且 对应的value是None
    // 那么这个key的全部小于等于a体系的都可以干掉了
    for memTableR in memTableRs.iter() {
        for (key, value) in memTableR.iter() {
            cursor.seek(key, true, value)?;
        }
    }

    let mut pageId2InvolvedParentPages = HashMap::new();

    let values: Vec<(Arc<RwLock<Page>>, Option<usize>)> = cursor.pageId2PageAndIndexInParent.into_values().collect();
    writePages(db, values, &mut pageId2InvolvedParentPages)?;

    loop {
        let involvedParentPagesPrevRound: Vec<Arc<RwLock<Page>>> = {
            let pairs: Vec<(PageId, Arc<RwLock<Page>>)> = pageId2InvolvedParentPages.drain().collect();
            pairs.into_iter().map(|(_, involvedParentPage)| involvedParentPage).collect()
        };

        if involvedParentPagesPrevRound.is_empty() {
            break;
        }

        let page2IndexInParentPage =
            involvedParentPagesPrevRound.into_iter().map(|involvedParentPage| {
                let indexInParentPage = {
                    let pageReadGuard = involvedParentPage.read().unwrap();
                    pageReadGuard.indexInParentPage
                };

                (involvedParentPage, indexInParentPage)
            }).collect::<Vec<_>>();

        writePages(db, page2IndexInParentPage, &mut pageId2InvolvedParentPages)?;
    }

    // memTableRs中的元素已经全部落地了,通知对应的immutable memTable
    for memTableR in memTableRs.iter() {
        let header: &mut MemTableFileHeader = utils::slice2RefMut(&memTableR.memTableFileMmap);
        header.written2Disk = true
    }

    Ok(())
}

fn writePages(db: &DB,
              writeDestPage2IndexInParentPage: Vec<(Arc<RwLock<Page>>, Option<usize>)>,
              involvedParentPages: &mut HashMap<PageId, Arc<RwLock<Page>>>) -> Result<()> {
    for (writeDestPage0, indexInParentPage) in writeDestPage2IndexInParentPage.iter() {
        let mut writeDestPage = writeDestPage0.write().unwrap();
        let writeDestPage = writeDestPage.deref_mut();

        writeDestPage.write2Disk(&db)?;

        // 原来的单个的leaf 现在成了多个 需要原来的那个单个的leaf的parantPage来应对
        // 得要知道当前的这个page在父级的那个位置,然后在对应的位置塞入data
        // 例如 原来 这个leafPage在它的上级中是对应(700,750]的
        // 现在的话(700,750]这段区间又要分裂了
        // 原来是单单这1个区间对应1个leaf,现在是分成多个各自对应单个leaf的

        // 现在要知道各个分裂出来的leafPage的最大的key是多少
        // 要现在最底下的writeDestLeafPage层上平坦横向scan掉然后再到上级的

        // 读取当前的additionalPage的最大的key
        // 是否有必要通过读取mmap来知道,能不能通过直接通过pageElems属性得到啊

        // 对branch来说相当重要的是key不能含有txId
        let getLastKeyInPage =
            |page: &Page| -> Result<Vec<u8>> {
                let last = match page.pageElems.last().unwrap() {
                    PageElem::LeafR(k, _) => *k,
                    PageElem::Dummy4PutLeaf(k, _) => k.as_slice(),
                    //
                    PageElem::LeafOverflowR(k, _) => *k,
                    PageElem::Dummy4PutLeafOverflow(k, _, _) => k.as_slice(),
                    //
                    PageElem::BranchR(k, _) => *k,
                    PageElem::Dummy4PutBranch(k, _) => k.as_slice(),
                    PageElem::Dummy4PutBranch0(k, _) => k.as_slice(),
                };

                //Ok(tx::extractKeyFromKeyWithTxId(last).to_vec())
                Ok(last.to_vec())
            };

        // 当前的page塞不下了,实际要分裂成多个了,需要再在上头盖1个branch
        // 如果说rootPage还是容纳的下 那么不用去理会了
        let needBuildNewParentPage =
            if writeDestPage.additionalPages.len() > 0 {
                writeDestPage.parentPage.is_none()
            } else {
                false
            };

        let parentPageId =
            // 到了顶头没有上级了
            if needBuildNewParentPage {
                let newParentPage = db.allocateNewPage(page_header::PAGE_FLAG_BRANCH)?;

                let pageId = newParentPage.header.id;

                // 因为rootPage变动了,dbHeader的rootPageId也要相应的变化
                db.getHeaderMut().rootPageId = pageId;

                writeDestPage.parentPage = Some(Arc::new(RwLock::new(newParentPage)));

                pageId
            } else {
                match writeDestPage.parentPage {
                    Some(ref parentPage) => parentPage.read().unwrap().header.id,
                    None => continue,
                }
            };

        let parentPage = writeDestPage.parentPage.as_ref().unwrap();

        // 添加之前先瞧瞧是不是已经有了相应的pageId了
        if involvedParentPages.contains_key(&parentPageId) == false {
            involvedParentPages.insert(parentPageId, parentPage.clone());
        }

        let mut parentPage = parentPage.write().unwrap();

        // 不应该使用insert,而是应直接替换的
        let mut indexInParentPage =
            {
                let lastKeyInPage = getLastKeyInPage(writeDestPage)?;

                match indexInParentPage {
                    Some(indexInParentPage) => {
                        if needBuildNewParentPage {
                            parentPage.pageElems.push(PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPage0.clone()));
                            0
                        } else {
                            let indexInParentPage = *indexInParentPage;
                            parentPage.pageElems[indexInParentPage] = PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPage0.clone());
                            indexInParentPage
                        }
                    }
                    None => { // 说明直接到了leafPage未经过branch 要么 这是1个临时新建的branch
                        parentPage.pageElems.push(PageElem::Dummy4PutBranch(lastKeyInPage, writeDestPage0.clone()));
                        0
                    }
                }
            };

        // 如果还有additionalPage的话,就要在indexInParentPage后边不断的塞入的
        // 使用drain是因为原来的测试是put之后重启在测试get,如果不重启直接get需要这样干清理掉的
        for additionalPage in writeDestPage.additionalPages.drain(..) {
            indexInParentPage += 1;

            let lastKeyInPage = getLastKeyInPage(&additionalPage)?;

            // insert的目标index可以和len相同,塞到最后的和push相同
            parentPage.pageElems.insert(indexInParentPage, PageElem::Dummy4PutBranch0(lastKeyInPage, additionalPage.header.id))
        }
    }

    Ok(())
}