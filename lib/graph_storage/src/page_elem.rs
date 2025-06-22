/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use std::sync::{Arc, RwLock};
use crate::page::Page;
use crate::page_header;
use crate::types::PageId;

/// table(文件)->block   block(文件)->page
///
/// 目前对leaf节点的保存思路如下
/// 如果value比较小 kv可以1道保存
/// 如果value比较大(那多少算是大呢,目前暂时定为pageSize的25%) value保存到单独的文件 leaf节点本身保存data的pos的
pub(crate) enum PageElem<'a> {
    /// key is with txId
    LeafR(&'a [u8], &'a [u8]),
    Dummy4PutLeaf(Vec<u8>, Vec<u8>),

    /// (key, value在文件中的位置的)
    LeafOverflowR(&'a [u8], usize),
    /// (key, pos, val)
    Dummy4PutLeafOverflow(Vec<u8>, usize, Vec<u8>),

    /// key is with txId
    BranchR(&'a [u8], PageId),
    Dummy4PutBranch(Vec<u8>, Arc<RwLock<Page>>),
    Dummy4PutBranch0(Vec<u8>, PageId),
}

impl<'a> PageElem<'a> {
    pub(crate) fn asBranchR(&self) -> anyhow::Result<(&'a [u8], PageId)> {
        if let PageElem::BranchR(keyWithoutTxId, pageId) = self {
            return Ok((keyWithoutTxId, *pageId));
        }

        throw!("a")
    }

    /// 传入的dest的len已经是pageElemDiskSize了
    pub(crate) fn write2Disk<'b>(&self, dest: &'b mut [u8]) -> anyhow::Result<&'b [u8]> {
        // 变为vec 这样的只要不断的push便可以了
        match self {
            PageElem::LeafR(k, v) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);

                // 因为传入的dest的len已经限制为pageElemDiskSize,不需要&mut dest[k.len()..k.len()+v.len()]
                valSlice.copy_from_slice(v);

                Ok(keySlice)
            }
            PageElem::Dummy4PutLeaf(k, v) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);
                valSlice.copy_from_slice(v);

                Ok(keySlice)
            }
            PageElem::LeafOverflowR(k, pos) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);
                valSlice.copy_from_slice(&pos.to_be_bytes());

                Ok(keySlice)
            }
            PageElem::Dummy4PutLeafOverflow(k, pos, _) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);
                valSlice.copy_from_slice(&pos.to_be_bytes());

                Ok(keySlice)
            }
            PageElem::BranchR(k, pageId) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);
                valSlice.copy_from_slice(&pageId.to_be_bytes());

                Ok(keySlice)
            }
            PageElem::Dummy4PutBranch(k, page) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);

                {
                    let page = page.read().unwrap();
                    valSlice.copy_from_slice(&page.header.pageId.to_be_bytes())
                }

                Ok(keySlice)
            }
            PageElem::Dummy4PutBranch0(k, pageId) => {
                let (keySlice, valSlice) = dest.split_at_mut(k.len());

                keySlice.copy_from_slice(k);
                valSlice.copy_from_slice(&pageId.to_be_bytes());

                Ok(keySlice)
            }
        }
    }

    /// 含有 pageElemMeta
    pub(crate) fn diskSize(&self) -> usize {
        match self {
            PageElem::LeafR(k, v) => page_header::LEAF_ELEM_META_SIZE + k.len() + v.len(),
            PageElem::Dummy4PutLeaf(k, v) => page_header::LEAF_ELEM_META_SIZE + k.len() + v.len(),
            //
            PageElem::LeafOverflowR(k, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            PageElem::Dummy4PutLeafOverflow(k, _, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            //
            PageElem::BranchR(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len() + size_of::<PageId>(),
            PageElem::Dummy4PutBranch(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len() + size_of::<PageId>(),
            PageElem::Dummy4PutBranch0(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len() + size_of::<PageId>(),
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