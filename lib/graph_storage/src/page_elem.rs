/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use crate::{page_header, utils};
use crate::page_header::{PageElemHeaderBranch, PageElemHeaderLeaf, PageElemHeaderLeafOverflow, PageHeader};
use crate::types::PageId;

/// table(文件)->block   block(文件)->page
///
/// 目前对leaf节点的保存思路如下
/// 如果value比较小 kv可以1道保存
/// 如果value比较大(那多少算是大呢,目前暂时定为pageSize的25%) value保存到单独的文件 leaf节点本身保存data的pos的
pub(crate) enum PageElem<'a> {
    /// key is with txId
    LeafR(&'a [u8], Option<&'a [u8]>),
    LeafRClone(Vec<u8>, Option<Vec<u8>>),
    Dummy4PutLeaf(Vec<u8>, Option<Vec<u8>>),

    /// (key, value在文件中的位置的)
    LeafOverflowR(&'a [u8], usize),
    LeafOverflowRClone(Vec<u8>, usize),
    /// (key, pos, val)
    Dummy4PutLeafOverflow(Vec<u8>, usize, Vec<u8>),

    /// branch体系的key不应含有txId
    BranchR(&'a [u8], PageId),
    BranchRClone(Vec<u8>, PageId),
    /// 是不是可以将page的header给暴露出来到tuple中,这样的话可以直接简单得到page的id
    /// 它含有的page是从0到有生成出来的 不是通过getPageById的
    Dummy4PutBranch(Vec<u8>, PageHeader),
}

impl<'a> PageElem<'a> {
    /// 传入的dest的len已经是pageElemDiskSize了,它是含有pageElemMeta的
    pub(crate) fn write2Disk<'b>(&self, dest: &'b mut [u8]) -> anyhow::Result<&'b [u8]> {

        // 变为vec 这样的只要不断的push便可以了
        match self {
            PageElem::LeafR(k, v) => {
                let (pageElemMetaSlice, kvSlice) = dest.split_at_mut(page_header::LEAF_ELEM_META_SIZE);

                let pageElemMetaLeaf: &mut PageElemHeaderLeaf = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeaf.keySize = k.len() as u16;
                pageElemMetaLeaf.valueSize = v.map_or_else(|| { 0 }, |v| v.len()) as u32;

                let (keySlice, valSlice) = kvSlice.split_at_mut(k.len());

                // page落地的时候,如果落在了和原来相同的底层的mmap上 且 这个pageElem落在mmap的位置和原来的相同
                // 那么什么都不用copy了,而且也不能,不然会报错, copy_from_slice 要求两段内存是不能overlap的
                if keySlice.as_ptr() == (*k).as_ptr() {
                    return Ok(keySlice);
                }

                // 到了这里如果不对key和value来clone,可能会产生overlap错误
                //
                // keySlice 和 k其实都是指向的相同的mmap内存
                // 如果原本是leafR的位置在经过了后续修改后变为Dummy4PutLeaf(key,none)
                // pageElem的大小由原来的LeafR(k,Some(v))的32字节变为Dummy4PutLeaf(key,none)的24字节
                // keySlice的位置提前k的位置8个字节,k的大小是16字节,自然会overlap了
                // 这样的话只能通过clone来应对
                // 同样的,branchR也得要这样的
                keySlice.copy_from_slice(k.to_vec().as_slice());

                // 因为传入的dest的len已经限制为pageElemDiskSize,不需要&mut dest[k.len()..k.len()+v.len()]
                if let Some(v) = v {
                    valSlice.copy_from_slice(v.to_vec().as_slice());
                }

                Ok(keySlice)
            }
            PageElem::LeafRClone(k, v) |
            PageElem::Dummy4PutLeaf(k, v) => {
                let (pageElemMetaSlice, kvSlice) = dest.split_at_mut(page_header::LEAF_ELEM_META_SIZE);

                let pageElemMetaLeaf: &mut PageElemHeaderLeaf = utils::slice2RefMut(pageElemMetaSlice);
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

                let pageElemMetaLeafOverflow: &mut PageElemHeaderLeafOverflow = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaLeafOverflow.keySize = k.len() as u16;
                pageElemMetaLeafOverflow.valPos = *valPos;

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            PageElem::LeafOverflowRClone(k, valPos) |
            PageElem::Dummy4PutLeafOverflow(k, valPos, _) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::LEAF_ELEM_OVERFLOW_META_SIZE);

                let pageElemMetaLeafOverflow: &mut PageElemHeaderLeafOverflow = utils::slice2RefMut(pageElemMetaSlice);
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

                let pageElemMetaBranch: &mut PageElemHeaderBranch = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaBranch.keySize = k.len() as u16;
                pageElemMetaBranch.pageId = *pageId;

                keySlice.copy_from_slice(k.to_vec().as_slice());

                Ok(keySlice)
            }
            PageElem::Dummy4PutBranch(k, childPageHeader) => {
                let (pageElemMetaSlice, keySlice) = dest.split_at_mut(page_header::BRANCH_ELEM_META_SIZE);

                let pageElemMetaBranch: &mut PageElemHeaderBranch = utils::slice2RefMut(pageElemMetaSlice);
                pageElemMetaBranch.keySize = k.len() as u16;
                pageElemMetaBranch.pageId = childPageHeader.id;
                /*{
                    let childPage = childPage.read().unwrap();
                    pageElemMetaBranch.pageId = childPage.header.id;
                }*/

                keySlice.copy_from_slice(k);

                Ok(keySlice)
            }
            _ => unimplemented!(),
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
            PageElem::LeafRClone(k, v) |
            PageElem::Dummy4PutLeaf(k, v) => {
                page_header::LEAF_ELEM_META_SIZE +
                    k.len() +
                    v.as_ref().map_or_else(|| { 0 }, |v| v.len())
            }
            //
            PageElem::LeafOverflowR(k, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            PageElem::LeafOverflowRClone(k, _) |
            PageElem::Dummy4PutLeafOverflow(k, _, _) => page_header::LEAF_ELEM_OVERFLOW_META_SIZE + k.len() + size_of::<usize>(),
            //
            PageElem::BranchR(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len(), // + size_of::<PageId>(),
            PageElem::BranchRClone(k, _) |
            PageElem::Dummy4PutBranch(k, _) => page_header::BRANCH_ELEM_META_SIZE + k.len(), // + size_of::<PageId>(),
            //_ => unimplemented!(),
        }
    }

    pub(crate) fn getKey(&self) -> &[u8] {
        match self {
            PageElem::LeafR(k, _) => k,
            PageElem::LeafRClone(k, _) => k,
            PageElem::Dummy4PutLeaf(k, _) => k,
            PageElem::LeafOverflowR(k, _) => k,
            PageElem::LeafOverflowRClone(k, _) => k,
            PageElem::Dummy4PutLeafOverflow(k, _, _) => k,
            PageElem::BranchR(k, _) => k,
            PageElem::BranchRClone(k, _) => k,
            PageElem::Dummy4PutBranch(k, _) => k,
            //_ => unimplemented!(),
        }
    }
}

impl<'a> Clone for PageElem<'a> {
    fn clone(&self) -> Self {
        match self {
            PageElem::LeafR(key, value) => PageElem::LeafRClone(key.to_vec(), value.map(|v| v.to_vec())),
            PageElem::LeafRClone(key, value) => PageElem::LeafRClone(key.clone(), value.clone()),
            PageElem::Dummy4PutLeaf(key, value) => PageElem::Dummy4PutLeaf(key.clone(), value.clone()),
            PageElem::LeafOverflowR(key, pos) => PageElem::LeafOverflowRClone(key.to_vec(), pos.clone()),
            PageElem::LeafOverflowRClone(key, pos) => PageElem::LeafOverflowRClone(key.clone(), pos.clone()),
            PageElem::Dummy4PutLeafOverflow(key, pos, value) => PageElem::Dummy4PutLeafOverflow(key.clone(), pos.clone(), value.clone()),
            PageElem::BranchR(key, pageId) => PageElem::BranchRClone(key.to_vec(), pageId.clone()),
            PageElem::BranchRClone(key, pageId) => PageElem::BranchRClone(key.clone(), pageId.clone()),
            PageElem::Dummy4PutBranch(key, pageHeader) => PageElem::Dummy4PutBranch(key.clone(), pageHeader.clone()),
            //_ => unimplemented!(),
        }
    }
}