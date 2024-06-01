use std::sync::atomic::Ordering;
use bytes::BytesMut;
use crate::{global, keyPrefixAddRowId, meta, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta::Table;
use crate::parser::{Insert, Link};
use crate::types::{DataKey, KeyTag, KV, RowId};

impl <'session> CommandExecutor <'session> {
    /// 它本质是向relation对应的data file写入
    /// 两个元素之间的relation只看种类不看里边的属性的
    pub (super) fn link(&self, link: &Link) -> anyhow::Result<CommandExecResult> {
        // 得到3个表的对象
        let srcTable = self.getTableRefByName(link.srcTableName.as_str())?;
        let destTable = self.getTableRefByName(link.destTableName.as_str())?;

        // 对src table和dest table调用expr筛选
        let srcSatisfiedVec = self.scanSatisfiedRows(srcTable.value(), link.srcTableFilterExpr.as_ref(), None, false, None)?;
        // src 空的 link 不成立
        if srcSatisfiedVec.is_empty() {
            return Ok(CommandExecResult::DmlResult);
        }

        let destSatisfiedVec = self.scanSatisfiedRows(destTable.value(), link.destTableFilterExpr.as_ref(), None, false, None)?;
        // dest 空的 link 不成立
        if destSatisfiedVec.is_empty() {
            return Ok(CommandExecResult::DmlResult);
        }

        // add rel本身的data
        let mut insertValues = Insert {
            tableName: link.relationName.clone(),
            useExplicitColumnNames: true,
            columnNames: link.relationColumnNames.clone(),
            columnExprs: link.relationColumnExprs.clone(),
        };

        let relation = self.getTableRefByName(&link.relationName)?;

        let relRowId: RowId = relation.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let relDataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, relRowId);

        let dataAdd = {
            let rowDataBinary = self.generateInsertValuesBinary(&mut insertValues, &*relation)?;
            (u64ToByteArrRef!(relDataKey).to_vec(), rowDataBinary.to_vec()) as KV
        };

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, relDataKey)?;

        let origin: KV = self.generateOrigin(relDataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&relation.name, dataAdd, xminAdd, xmaxAdd, origin);

        //--------------------------------------------------------------------

        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

        let mut process = |selfTable: &Table, selfDataKey: DataKey, pointerKeyTag: KeyTag, targetTable: &Table, targetDataKey: DataKey| {
            let (xmin, xmax) = self.generateAddPointerXminXmax(&mut pointerKeyBuffer, selfDataKey, pointerKeyTag, targetTable.tableId, targetDataKey)?;
            self.session.writeAddPointerMutation(&selfTable.name, xmin, xmax);

            anyhow::Result::<()>::Ok(())
        };

        // 对src来说
        // key + rel的tableId + rel的key
        {
            // todo 要是srcSatisfiedVec太大如何应对 挨个遍历set不现实
            // 尚未设置过滤条件 得到的是全部的
            if srcSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for srcDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(srcTable.value(), srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation.value(), relDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedVec {
                    process(srcTable.value(), *srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation.value(), relDataKey)?;
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
                    process(relation.value(), relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.value(), srcDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedVec {
                    process(relation.value(), relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.value(), *srcDataKey)?;
                }
            }

            if destSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(relation.value(), relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable.value(), destDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedVec {
                    process(relation.value(), relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable.value(), *destDataKey)?;
                }
            }
        }

        // 对dest来说
        // key + rel的tableId + rel的key
        {
            if destSatisfiedVec[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedVec[1].0..=srcSatisfiedVec[2].0 {
                    process(destTable.value(), destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation.value(), relDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedVec {
                    process(destTable.value(), *destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation.value(), relDataKey)?;
                }
            }
        }

        Ok(CommandExecResult::DmlResult)
    }
}