use std::ptr::NonNull;
use crate::page_header::{PageElemMeta, PageHeader};
use crate::{page_header, utils};
use anyhow::Result;
use memmap2::{MmapMut};
use std::sync::{Arc, RwLock};
use crate::db::DB;
use crate::page_elem::PageElem;

pub(crate) const OVERFLOW_DIVIDE: usize = 100;

/// page presentation in memory
pub(crate) struct Page {
    // #[deprecated]
    // pub(crate) parentPage: Option<Arc<RwLock<Page>>>,

    // #[deprecated]
    /// 专门用来traverse tree时候使用的
    // pub(crate) indexInParentPage: Option<usize>,

    /// if page is dummy then it is none
    pub(crate) mmapMut: MmapMut,

    /// 无中生有通过mmapMut得到的
    pub(crate) header: &'static mut PageHeader,

    pub(crate) pageElems: Vec<PageElem<'static>>,

    //pub(crate) childPages: Option<Vec<Arc<Page>>>,

    // 用来容纳seek时候落地写的时候本身的1个mmapMut以外多出来的page
    pub(crate) additionalPages: Vec<Page>,
}

impl Page {
    #[inline]
    pub(crate) fn isLeaf(&self) -> bool {
        self.header.isLeaf()
    }

    #[inline]
    pub(crate) fn isDummy(&self) -> bool {
        self.header.isDummy()
    }

    /// 计算当前page含有的内容大小,是含有pageHeader的
    pub(crate) fn diskSize(&self) -> usize {
        let mut size = page_header::PAGE_HEADER_SIZE;

        for pageElem in &self.pageElems {
            size += pageElem.diskSize();
        }

        size
    }

    /// 不含有pageHeader,只是纯的payload的
    pub(crate) fn payloadDiskSize(&self) -> usize {
        let mut size = 0;

        for pageElem in &self.pageElems {
            size += pageElem.diskSize();
        }

        size
    }

    pub(crate) fn get1stElemMeta(&self) -> Result<&dyn PageElemMeta> {
        let pageHeader = utils::slice2Ref::<PageHeader>(self.mmapMut.as_ref());
        let pageElemMeta = pageHeader.readPageElemMeta(0)?;
        Ok(pageElemMeta)
    }

    pub(crate) fn getLastElemMeta(&self) -> Result<&dyn PageElemMeta> {
        let pageHeader = utils::slice2Ref::<PageHeader>(self.mmapMut.as_ref());
        let pageElemMeta = pageHeader.readPageElemMeta(pageHeader.elemCount as usize - 1)?;
        Ok(pageElemMeta)
    }

    pub(crate) fn write2Disk(&mut self, db: &DB) -> Result<usize> {
        let pageSize = db.getHeader().pageSize;

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

        // 需提前知道会不会分裂,以确定真正的pageSize大小
        let pageAvailableSize = {
            let mut curPosInPage = curPosInPage;

            let mut needSplitPage = false;

            // 不断遍历累加pageElem大小
            // 为何不直接使用page.diskSize(),因为它内部实现也是遍历累加的,可能会有不必要的过多的遍历
            for pageElem in &self.pageElems {
                curPosInPage += pageElem.diskSize();

                if curPosInPage > pageSize {
                    needSplitPage = true;
                    break;
                }
            }

            // 如果会分裂的话,需要对分裂出来的page的填充率进行限制的
            if needSplitPage {
                f64::ceil(pageSize as f64 * db.dbOption.pageFillPercentAfterSplit) as usize
            } else {
                pageSize
            }
        };

        for (index, pageElem) in self.pageElems.iter().enumerate() {
            let pageElemDiskSize = pageElem.diskSize();

            // 当前page剩下空间不够了,需要分配1个page
            if curPosInPage + pageElemDiskSize > pageAvailableSize {
                splitIndices.push(index);

                curPage = {
                    // 上1个的page写满之后的处理的
                    writePageHeader(elementCount, unsafe { &curPage.as_ref().mmapMut });

                    // 说明原来的leafPage空间不够了,分裂出了又1个leafPage
                    let additionalPage = db.allocateNewPage(page_header::PAGE_FLAG_LEAF)?;
                    self.additionalPages.push(additionalPage);

                    NonNull::new(self.additionalPages.last_mut().unwrap() as *mut _).unwrap()
                };

                // 给pageHeader保留
                curPosInPage = page_header::PAGE_HEADER_SIZE;
                elementCount = 0;
            }

            // 实际写入各个pageElem
            let destSlice = {
                let curPageMmapMut = unsafe { curPage.as_mut().mmapMut.as_mut() };
                &mut curPageMmapMut[curPosInPage..curPosInPage + pageElemDiskSize]
            };

            // 这个引用的源头不是pageElem而是写入目的地的
            // destSlice是包含了pageElemMeta的
            pageElem.write2Disk(destSlice)?;

            curPosInPage += pageElemDiskSize;
            elementCount += 1;
        }

        // 全部的pageElem写完后的处理的
        writePageHeader(elementCount, unsafe { &curPage.as_ref().mmapMut });

        // 分配切分pageElems到各个additionalPage
        // 使用倒序
        for (splitIndex, additionalPage) in
            splitIndices.into_iter().rev().zip(self.additionalPages.iter_mut().rev()) {
            // split_off() 返回的是后半部分
            additionalPage.pageElems = self.pageElems.split_off(splitIndex);
        }

        Ok(pageAvailableSize)
    }

    #[inline]
    pub(crate) fn msync(&self) -> Result<()> {
        self.mmapMut.flush()?;
        Ok(())
    }

    pub(crate) fn getLastKey(&self) -> Vec<u8> {
        let last =
            match self.pageElems.last().unwrap() {
                PageElem::LeafR(k, _) => *k,
                PageElem::Dummy4PutLeaf(k, _) => k.as_slice(),
                //
                PageElem::LeafOverflowR(k, _) => *k,
                PageElem::Dummy4PutLeafOverflow(k, _, _) => k.as_slice(),
                //
                PageElem::BranchR(k, _) => *k,
                PageElem::Dummy4PutBranch(k, _, _) => k.as_slice(),
                PageElem::Dummy4PutBranch0(k, _) => k.as_slice(),
            };

        last.to_vec()
    }
}

impl TryFrom<MmapMut> for Page {
    type Error = anyhow::Error;

    fn try_from(mmapMut: MmapMut) -> Result<Self, Self::Error> {
        let pageHeader = utils::slice2RefMut::<PageHeader>(&mmapMut);

        let pageElems = {
            let pageHeader = utils::slice2RefMut::<PageHeader>(&mmapMut);

            let mut pageElems = Vec::with_capacity(pageHeader.elemCount as usize);

            for index in 0..pageHeader.elemCount as usize {
                let pageElemMeta = pageHeader.readPageElemMeta(index)?;
                let pageElem = pageElemMeta.readPageElem();
                pageElems.push(pageElem);
            }

            pageElems
        };

        Ok(Page {
            //parentPage: None,
            //indexInParentPage: None,
            mmapMut,
            header: pageHeader,
            pageElems,
            //childPages: None,
            additionalPages: vec![],
        })
    }
}