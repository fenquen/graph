/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use std::sync::{Arc, RwLock};
use crate::page::Page;
use crate::{page_header, utils};
use crate::page_header::{PageElemMetaBranch, PageElemMetaLeaf, PageElemMetaLeafOverflow};
use crate::types::PageId;

/// table(文件)->block   block(文件)->page
///
/// 目前对leaf节点的保存思路如下
/// 如果value比较小 kv可以1道保存
/// 如果value比较大(那多少算是大呢,目前暂时定为pageSize的25%) value保存到单独的文件 leaf节点本身保存data的pos的
pub(crate) enum PageElem<'a> {
    /// key is with txId
    LeafR(&'a [u8], Option<&'a [u8]>),
    Dummy4PutLeaf(Vec<u8>, Option<Vec<u8>>),

    /// (key, value在文件中的位置的)
    LeafOverflowR(&'a [u8], usize),
    /// (key, pos, val)
    Dummy4PutLeafOverflow(Vec<u8>, usize, Vec<u8>),

    /// branch体系的key不应含有txId
    BranchR(&'a [u8], PageId),
    Dummy4PutBranch(Vec<u8>, Arc<RwLock<Page>>),
    Dummy4PutBranch0(Vec<u8>, PageId),
}

impl<'a> PageElem<'a> {
    /// 传入的dest的len已经是pageElemDiskSize了,它是含有pageElemMeta的
    pub(crate) fn write2Disk<'b>(&self, dest: &'b mut [u8]) -> anyhow::Result<&'b [u8]> {
        // 变为vec 这样的只要不断的push便可以了
        match self {
            PageElem::LeafR(k, v) => {
                let (pageElemMetaSlice, kvSlice) = dest.split_at_mut(page_header::LEAF_ELEM_META_SIZE);

                let pageElemMetaLeaf: &mut PageElemMetaLeaf = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeaf.keySize = k.len() as u16;
                pageElemMetaLeaf.valueSize = v.map_or_else(|| { 0 }, |v| v.len()) as u32;

                let (keySlice, valSlice) = kvSlice.split_at_mut(k.len());

                // page落地的时候,如果落在了和原来相同的底层的mmap上 且 这个pageElem落在mmap的位置和原来的相同
                // 那么什么都不用copy了,而且也不能,不然会报错, copy_from_slice 要求两段内存是不能overlap的
                if keySlice.as_ptr() == (*k).as_ptr() {
                    return Ok(keySlice);
                }

                keySlice.copy_from_slice(k);

                // 因为传入的dest的len已经限制为pageElemDiskSize,不需要&mut dest[k.len()..k.len()+v.len()]
                if let Some(v) = v {
                    valSlice.copy_from_slice(v);
                }

                Ok(keySlice)
            }
            PageElem::Dummy4PutLeaf(k, v) => {
                let (pageElemMetaSlice, kvSlice) = dest.split_at_mut(page_header::LEAF_ELEM_META_SIZE);

                let pageElemMetaLeaf: &mut PageElemMetaLeaf = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeaf.keySize = k.len() as u16;
                pageElemMetaLeaf.valueSize = v.as_ref().map_or_else(|| { 0 }, |v| v.len()) as u32;

                let (keySlice, valSlice) = kvSlice.split_at_mut(k.len());

                keySlice.copy_from_slice(k);

                if let Some(v) = v {
                    valSlice.copy_from_slice(v);
                }

                Ok(keySlice)
            }
            //---------------------------------------------------------
            PageElem::LeafOverflowR(k, valPos) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::LEAF_ELEM_OVERFLOW_META_SIZE);

                if keySlice.as_ptr() == (*k).as_ptr() {
                    return Ok(keySlice);
                }

                let pageElemMetaLeafOverflow: &mut PageElemMetaLeafOverflow = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeafOverflow.keySize = k.len() as u16;
                pageElemMetaLeafOverflow.valPos = *valPos;

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            PageElem::Dummy4PutLeafOverflow(k, valPos, _) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::LEAF_ELEM_OVERFLOW_META_SIZE);

                let pageElemMetaLeafOverflow: &mut PageElemMetaLeafOverflow = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeafOverflow.keySize = k.len() as u16;
                pageElemMetaLeafOverflow.valPos = *valPos;

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            //--------------------------------------------------------
            PageElem::BranchR(k, pageId) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::BRANCH_ELEM_META_SIZE);

                if keySlice.as_ptr() == (*k).as_ptr() {
                    return Ok(keySlice);
                }

                let pageElemMetaBranch: &mut PageElemMetaBranch = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaBranch.keySize = k.len() as u16;
                pageElemMetaBranch.pageId = *pageId;

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            PageElem::Dummy4PutBranch(k, childPage) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::BRANCH_ELEM_META_SIZE);

                let pageElemMetaBranch: &mut PageElemMetaBranch = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaBranch.keySize = k.len() as u16;
                {
                    let childPage = childPage.read().unwrap();
                    pageElemMetaBranch.pageId = childPage.header.id;
                }

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            PageElem::Dummy4PutBranch0(k, pageId) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::BRANCH_ELEM_META_SIZE);

                let pageElemMetaBranch: &mut PageElemMetaBranch = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaBranch.keySize = k.len() as u16;
                pageElemMetaBranch.pageId = *pageId;

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
        }
    }

    /// 含有 pageElemMeta
    pub(crate) fn diskSize(&self) -> usize {
        match self {
            PageElem::LeafR(k, v) => {
                page_header::LEAF_ELEM_META_SIZE +
                    k.len() +
                    v.map_or_else(|| { 0 }, |v| v.len())
            }
            PageElem::Dummy4PutLeaf(k, v) => {
                page_header::LEAF_ELEM_META_SIZE +
                    k.len() +
                    if let Some(v) = v {
                        v.len()
                    } else {
                        0
                    }
            }
            //
            PageElem::LeafOverflowR(k, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            PageElem::Dummy4PutLeafOverflow(k, _, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            //
            PageElem::BranchR(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len(), // + size_of::<PageId>(),
            PageElem::Dummy4PutBranch(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len(), // + size_of::<PageId>(),
            PageElem::Dummy4PutBranch0(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len(), // + size_of::<PageId>(),
        }
    }

    pub(crate) fn getKey(&self) -> &[u8] {
        match self {
            PageElem::LeafR(k, _) => k,
            PageElem::Dummy4PutLeaf(k, _) => k,
            PageElem::LeafOverflowR(k, _) => k,
            PageElem::Dummy4PutLeafOverflow(k, _, _) => k,
            PageElem::BranchR(k, _) => k,
            PageElem::Dummy4PutBranch(k, _) => k,
            PageElem::Dummy4PutBranch0(k, _) => k,
        }
    }
}