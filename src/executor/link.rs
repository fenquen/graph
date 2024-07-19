use std::sync::atomic::Ordering;
use bytes::BytesMut;
use log::Level::Debug;
use crate::{global, keyPrefixAddRowId, meta, types, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::executor::store::{ScanHooks, ScanParams};
use crate::meta::Table;
use crate::parser::command::insert::Insert;
use crate::parser::command::link::{Link, LinkTo};
use crate::types::{DataKey, KeyTag, KV, RowId, CommittedPreProcessor};
use anyhow::Result;
use crate::parser::command::select::SelectRel;
use crate::session::Session;

impl<'session> CommandExecutor<'session> {
    pub(super) fn link(&self, link: &Link) -> Result<CommandExecResult> {
        match link {
            Link::LinkTo(linkTo) => self.linkTo(linkTo),
            Link::LinkChain(selctRels) => self.linkChain(selctRels)
        }
    }

    fn linkTo(&self, linkToStyle: &LinkTo) -> Result<CommandExecResult> {
        // 得到表的对象
        let dbObjectSrcTable = Session::getDBObjectByName(linkToStyle.srcTableName.as_str())?;
        let srcTable = dbObjectSrcTable.asTable()?;

        let dbObjectDestTable = Session::getDBObjectByName(linkToStyle.destTableName.as_str())?;
        let destTable = dbObjectDestTable.asTable()?;

        // 对src table和dest table调用expr筛选
        let srcSatisfiedVec = {
            let scanParams = ScanParams {
                table: srcTable,
                tableFilter: linkToStyle.srcTableFilterExpr.as_ref(),
                ..Default::default()
            };

            let srcSatisfiedVec = self.scanSatisfiedRows(scanParams, false, ScanHooks::default())?;

            // src 空的 link 不成立
            if srcSatisfiedVec.is_empty() {
                return Ok(CommandExecResult::DmlResult);
            }

            srcSatisfiedVec
        };

        let destSatisfiedVec = {
            let scanParams = ScanParams {
                table: destTable,
                tableFilter: linkToStyle.destTableFilterExpr.as_ref(),
                ..Default::default()
            };

            let destSatisfiedVec = self.scanSatisfiedRows(scanParams, false, ScanHooks::default())?;

            // dest 空的 link 不成立
            if destSatisfiedVec.is_empty() {
                return Ok(CommandExecResult::DmlResult);
            }

            destSatisfiedVec
        };

        // add rel本身的data
        let mut insertValues = Insert {
            tableName: linkToStyle.relationName.clone(),
            useExplicitColumnNames: true,
            columnNames: linkToStyle.relationColumnNames.clone(),
            columnExprVecVec: vec![linkToStyle.relationColumnExprs.clone()],
        };

        let dbObjectRelation = Session::getDBObjectByName(&linkToStyle.relationName)?;
        let relation = dbObjectRelation.asRelation()?;

        let relRowId: RowId = relation.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let relDataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, relRowId);

        let dataAdd = {
            let (rowDataBinary, _) = &self.generateInsertValuesBinary(&mut insertValues, &*relation)?[0];
            (u64ToByteArrRef!(relDataKey).to_vec(), rowDataBinary.to_vec()) as KV
        };

        let mut mvccKeyBuffer = self.withCapacityIn(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, relDataKey)?;

        let origin: KV = self.generateOrigin(relDataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&relation.name, dataAdd, xminAdd, xmaxAdd, origin);

        //--------------------------------------------------------------------

        let mut pointerKeyBuffer = self.withCapacityIn(meta::POINTER_KEY_BYTE_LEN);

        let mut process =
            |selfTable: &Table, selfDataKey: DataKey,
             pointerKeyTag: KeyTag,
             targetTable: &Table, targetDataKey: DataKey| {
                let (xmin, xmax) = self.generateAddPointerXminXmax(&mut pointerKeyBuffer, selfDataKey, pointerKeyTag, targetTable.id, targetDataKey)?;
                self.session.writeAddPointerMutation(&selfTable.name, xmin, xmax);

                Result::<()>::Ok(())
            };

        // 对src来说
        // key + rel的tableId + rel的key
        {
            // todo 要是srcSatisfiedVec太大如何应对 挨个遍历set不现实
            // 尚未设置过滤条件 得到的是全部的
            if srcSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for srcDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(srcTable, srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation, relDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedVec {
                    process(srcTable, *srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation, relDataKey)?;
                }
            }
        }

        // 对rel来说
        // key + src的tableId + src的key
        // key + dest的tableId + dest的key
        {
            // 尚未设置过滤条件 得到的是全部的
            if srcSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for srcDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(relation, relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable, srcDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedVec {
                    process(relation, relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable, *srcDataKey)?;
                }
            }

            if destSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(relation, relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable, destDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedVec {
                    process(relation, relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable, *destDataKey)?;
                }
            }
        }

        // 对dest来说
        // key + rel的tableId + rel的key
        {
            if destSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(destTable, destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation, relDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedVec {
                    process(destTable, *destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation, relDataKey)?;
                }
            }
        }

        Ok(CommandExecResult::DmlResult)
    }

    fn linkChain(&self, selctRels: &[SelectRel]) -> Result<CommandExecResult> {
        todo!()
    }
}