use std::{hint, mem};
use crate::page_header::{PageElemMeta, PageHeader};
use crate::{page_header, utils};
use anyhow::Result;
use memmap2::MmapMut;
use std::sync::{Arc, RwLock};
use crate::db::DB;
use crate::page_elem::PageElem;

pub(crate) const OVERFLOW_DIVIDE: usize = 100;

/// page presentation in memory
pub(crate) struct Page {
    pub(crate) parentPage: Option<Arc<RwLock<Page>>>,

    /// 专门用来traverse tree时候使用的
    pub(crate) indexInParentPage: Option<usize>,

    /// if page is dummy then it is none
    pub(crate) mmapMut: Option<MmapMut>,

    /// 无中生有通过mmapMut得到的
    pub(crate) header: &'static PageHeader,

    pub(crate) pageElems: Vec<PageElem<'static>>,

    // 以下的两个的赋值应该是以本struct内的mmapMut来源
    // pub(crate) keyMin: Option<&'static [u8]>,
    // pub(crate) keyMax: Option<&'static [u8]>,

    pub(crate) childPages: Option<Vec<Arc<Page>>>,

    // 用来容纳seek时候落地写的时候本身的1个mmapMut以外多出来的page
    pub(crate) additionalPages: Vec<Page>,

    //pub(crate) dirty: bool,
}

// pub(crate) fn
impl Page {
    pub(crate) fn readFromMmap(mmapMut: MmapMut) -> Result<Page> {
        let pageHeader = utils::slice2Ref::<PageHeader>(&mmapMut);

        let pageElemVec = {
            let mut pageElemVec = Vec::with_capacity(pageHeader.elemCount as usize);

            for index in 0..pageHeader.elemCount as usize {
                let pageElemMeta = pageHeader.readPageElemMeta(index)?;
                let pageElem = pageElemMeta.readPageElem();
                pageElemVec.push(pageElem);
            }

            pageElemVec
        };

        Ok(Page {
            parentPage: None,
            indexInParentPage: None,
            mmapMut: Some(mmapMut),
            header: pageHeader,
            pageElems: pageElemVec,
            //keyMin: Default::default(),
            //keyMax: Default::default(),
            childPages: None,
            additionalPages: vec![],
        })
    }

    pub(crate) fn buildDummyLeafPage() -> Page {
        Page {
            parentPage: None,
            indexInParentPage: None,
            mmapMut: None,
            header: &page_header::PAGE_HEADER_DUMMY_LEAF,
            pageElems: vec![],
            //keyMin: Default::default(),
            //keyMax: Default::default(),
            childPages: None,
            additionalPages: vec![],
        }
    }

    #[inline]
    pub(crate) fn isLeaf(&self) -> bool {
        self.header.isLeaf()
    }

    #[inline]
    pub(crate) fn isDummy(&self) -> bool {
        self.header.isDummy()
    }

    /// 计算当前page含有的内容需要用掉多少page
    pub(crate) fn diskSize(&self) -> usize {
        let mut size = page_header::PAGE_HEADER_SIZE;

        for pageElem in &self.pageElems {
            size += pageElem.diskSize();
        }

        size
    }

    pub(crate) fn get1stElemMeta(&self) -> Result<&dyn PageElemMeta> {
        let pageHeader = utils::slice2Ref::<PageHeader>(self.mmapMut.as_ref().unwrap());
        let pageElemMeta = pageHeader.readPageElemMeta(0)?;
        Ok(pageElemMeta)
    }

    pub(crate) fn getLastElemMeta(&self) -> Result<&dyn PageElemMeta> {
        let pageHeader = utils::slice2Ref::<PageHeader>(self.mmapMut.as_ref().unwrap());
        let pageElemMeta = pageHeader.readPageElemMeta(pageHeader.elemCount as usize - 1)?;
        Ok(pageElemMeta)
    }

    pub(crate) fn write(&mut self, db: &DB) -> Result<()> {
        let pageSize = db.getHeader().pageSize as usize;

        if self.isDummy() {
            self.mmapMut = Some(db.allocateNewPage()?);
        }

        let pageIsLeaf = self.isLeaf();

        let mut dummyPage = Page::buildDummyLeafPage();

        let mut curPage = &mut dummyPage;
        let mut firstPage = true;
        let mut curPosInPage = page_header::PAGE_HEADER_SIZE;
        let mut elementCount = 0;
        let mut firstElemInPage = false;

        let writePageHeader = |elementCount: usize, mmapMut: &MmapMut| {
            let pageHeader: &mut PageHeader = utils::slice2RefMut(mmapMut);

            // 写当前page的header
            pageHeader.flags = if pageIsLeaf {
                page_header::PAGE_FLAG_LEAF
            } else {
                page_header::PAGE_FLAG_BRANCH
            };

            pageHeader.elemCount = elementCount as u16;
        };

        let a = self as *mut _;

        for pageElem in self.pageElems.iter() {
            let pageElemDiskSize = pageElem.diskSize();

            // 当前page剩下空间不够了,需要分配1个page
            if hint::unlikely(curPosInPage + pageElemDiskSize > pageSize) {
                let curPageNew =
                    if firstPage {
                        firstPage = false;

                        // if pageIsDummy {
                        //     self.mmapMut = Some(db.allocateNewPage()?);
                        // }

                        a
                    } else {
                        // 上1个的page写满之后的处理的
                        writePageHeader(elementCount, curPage.mmapMut.as_ref().unwrap());

                        // 说明原来的leafPage空间不够了,分裂出了又1个leafPage
                        let mut additionalPage = Page::buildDummyLeafPage();
                        additionalPage.mmapMut = Some(db.allocateNewPage()?);
                        additionalPage.header = utils::slice2RefMut(additionalPage.mmapMut.as_ref().unwrap());

                        self.additionalPages.push(additionalPage);
                        self.additionalPages.last_mut().unwrap() as *mut _
                    };

                curPage = unsafe { mem::transmute(curPageNew) };

                // 给pageHeader保留
                curPosInPage = page_header::PAGE_HEADER_SIZE;
                elementCount = 0;
                firstElemInPage = true;
            } else {
                curPage = unsafe { mem::transmute(a) };
                firstPage = false;
            }

            // 实际写入各个pageElem
            let destSlice = {
                let curPageMmapMut = curPage.mmapMut.as_mut().unwrap();
                &mut curPageMmapMut[curPosInPage..curPosInPage + pageElemDiskSize]
            };

            // 这个引用的源头不是pageElem而是写入目的地的
            // destSlice是包含了pageElemMeta的
            pageElem.write2Disk(destSlice)?;

            // 如果写的是当前page的第1个的elem
            if firstElemInPage {
                firstElemInPage = false;
            }

            curPosInPage += pageElemDiskSize;
            elementCount += 1;
        }

        // 全部的pageElem写完后的处理的
        writePageHeader(elementCount, curPage.mmapMut.as_ref().unwrap());

        Ok(())
    }
}