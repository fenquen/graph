use std::{hint, mem};
use std::ptr::NonNull;
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

    pub(crate) fn write2Disk(&mut self, db: &DB) -> Result<()> {
        let pageSize = db.getHeader().pageSize as usize;

        if self.isDummy() {
            self.mmapMut = Some(db.allocateNewPage()?);
        }

        let pageIsLeaf = self.isLeaf();

        let mut curPage: NonNull<Page> = NonNull::new(self as *mut _).unwrap();
        let mut curPosInPage = page_header::PAGE_HEADER_SIZE;
        let mut elementCount = 0;

        // 如果page的内容太多要写到额外的page,那么pageElems保存的内容是要分派到各个page的
        let mut splitIndices = Vec::new();

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

        for (index, pageElem) in self.pageElems.iter().enumerate() {
            let pageElemDiskSize = pageElem.diskSize();

            // 当前page剩下空间不够了,需要分配1个page
            if curPosInPage + pageElemDiskSize > pageSize {
                splitIndices.push(index);

                curPage = {
                    // 上1个的page写满之后的处理的
                    writePageHeader(elementCount, unsafe { curPage.as_ref().mmapMut.as_ref().unwrap() });

                    // 说明原来的leafPage空间不够了,分裂出了又1个leafPage
                    let mut additionalPage = Page::buildDummyLeafPage();
                    additionalPage.mmapMut = Some(db.allocateNewPage()?);
                    additionalPage.header = utils::slice2RefMut(additionalPage.mmapMut.as_ref().unwrap());

                    self.additionalPages.push(additionalPage);
                    NonNull::new(self.additionalPages.last_mut().unwrap() as *mut _).unwrap()
                };

                // 给pageHeader保留
                curPosInPage = page_header::PAGE_HEADER_SIZE;
                elementCount = 0;
            }

            // 实际写入各个pageElem
            let destSlice = {
                let curPageMmapMut = unsafe { curPage.as_mut().mmapMut.as_mut().unwrap() };
                &mut curPageMmapMut[curPosInPage..curPosInPage + pageElemDiskSize]
            };

            // 这个引用的源头不是pageElem而是写入目的地的
            // destSlice是包含了pageElemMeta的
            pageElem.write2Disk(destSlice)?;

            curPosInPage += pageElemDiskSize;
            elementCount += 1;
        }

        // 全部的pageElem写完后的处理的
        writePageHeader(elementCount, unsafe { curPage.as_ref().mmapMut.as_ref().unwrap() });

        // 使用倒序
        for (splitIndex, additionalPage) in
            splitIndices.into_iter().rev().zip(self.additionalPages.iter_mut().rev()) {
            additionalPage.pageElems = self.pageElems.split_off(splitIndex);
        }

        Ok(())
    }
}

impl TryFrom<MmapMut> for Page {
    type Error = anyhow::Error;

    fn try_from(mmapMut: MmapMut) -> Result<Self, Self::Error> {
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
            childPages: None,
            additionalPages: vec![],
        })
    }
}