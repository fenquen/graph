use std::sync::atomic::Ordering;
use bytes::BytesMut;
use crate::{global, keyPrefixAddRowId, meta, types, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::executor::store::{ScanHooks, ScanParams};
use crate::meta::Table;
use crate::parser::command::insert::Insert;
use crate::parser::command::link::{Link, LinkTo};
use crate::types::{DataKey, KeyTag, KV, RowId, CommittedPreProcessor, RowData, SessionVec, DBObjectId};
use anyhow::Result;
use crate::parser::command::select::SelectRel;
use crate::session::{Mutation, Session};

impl<'session> CommandExecutor<'session> {
    pub(super) fn link(&self, link: &Link) -> Result<CommandExecResult> {
        // 要是偷懒的话vec直接保存String就可以了,不过还是想更优秀,去掉不必要的string的clone,下边会为了这个目的用到不少的显式生命周期标注
        let mut mutationsDest = self.vecNewIn();
        let mut linkToVec = Vec::new();

        let a = match link {
            Link::LinkTo(linkTo) => {
                let (commandExecResult, _) = self.linkTo(linkTo, None, &mut mutationsDest)?;
                Ok(commandExecResult)
            }
            Link::LinkChain(selctRels) => {
                linkToVec = selctRels.iter().map(|selectRel| {
                    let mut linkTo = LinkTo::default();

                    linkTo.srcTableName = selectRel.srcTableName.clone();
                    linkTo.srcTableFilter = selectRel.srcFilter.clone();

                    linkTo.destTableName = selectRel.destTableName.clone();
                    linkTo.destTableFilter = selectRel.destFilter.clone();

                    linkTo.relationName = selectRel.relationName.clone();
                    linkTo.relationColumnNames = selectRel.relationInsertColumnNames.as_ref().unwrap().clone();
                    linkTo.relationColumnExprs = selectRel.relationInsertColumnExprs.as_ref().unwrap().clone();

                    linkTo
                }).collect();

                self.linkChain(&linkToVec, &mut mutationsDest)
            }
        };

        for (tableName, mutation) in mutationsDest {
            self.session.writeMutation(tableName, mutation);
        }

        a
    }

    ///  link user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 12)
    /// 返回的是另外1个对象是 dest上的数据
    fn linkTo(&self,
              linkTo: &LinkTo,
              lastRoundDestSatisfiedDatas: Option<Vec<(DataKey, RowData)>>,
              mutationsDest: &mut SessionVec<(DBObjectId, Mutation)>) -> Result<(CommandExecResult, Option<Vec<(DataKey, RowData)>>)> {
        // 得到表的对象
        let dbObjectSrcTable = Session::getDBObjectByName(linkTo.srcTableName.as_str())?;
        let srcTable = dbObjectSrcTable.asTable()?;

        let dbObjectDestTable = Session::getDBObjectByName(linkTo.destTableName.as_str())?;
        let destTable = dbObjectDestTable.asTable()?;

        // 对src table和dest table调用expr筛选
        let srcSatisfiedDatas = {
            match lastRoundDestSatisfiedDatas {
                Some(lastRoundDestSatisfiedDatas) => lastRoundDestSatisfiedDatas,
                None => {
                    let scanParams = ScanParams {
                        table: srcTable,
                        tableFilter: linkTo.srcTableFilter.as_ref(),
                        ..Default::default()
                    };

                    let srcSatisfiedDatas = self.scanSatisfiedRows(scanParams, false, ScanHooks::default())?;

                    // src 空的 link 不成立
                    if srcSatisfiedDatas.is_empty() {
                        return Ok((CommandExecResult::DmlResult, None));
                    }

                    srcSatisfiedDatas
                }
            }
        };

        let destSatisfiedDatas = {
            let scanParams = ScanParams {
                table: destTable,
                tableFilter: linkTo.destTableFilter.as_ref(),
                ..Default::default()
            };

            let destSatisfiedDatas = self.scanSatisfiedRows(scanParams, false, ScanHooks::default())?;

            // dest 空的 link 不成立
            if destSatisfiedDatas.is_empty() {
                return Ok((CommandExecResult::DmlResult, None));
            }

            destSatisfiedDatas
        };

        // add rel本身的data
        let mut insertValues = Insert {
            tableName: linkTo.relationName.clone(),
            useExplicitColumnNames: true,
            columnNames: linkTo.relationColumnNames.clone(),
            columnExprVecVec: vec![linkTo.relationColumnExprs.clone()],
        };

        let dbObjectRelation = Session::getDBObjectByName(&linkTo.relationName)?;
        let relation = dbObjectRelation.asRelation()?;

        // 得到相应的dataKey
        let relRowId: RowId = relation.nextRowId();
        let relDataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, relRowId);

        let dataAdd = {
            let (rowDataBinary, _) = &self.generateInsertValuesBinary(&mut insertValues, &*relation)?[0];
            (u64ToByteArrRef!(relDataKey).to_vec(), rowDataBinary.to_vec()) as KV
        };

        let mut mvccKeyBuffer = self.withCapacityIn(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, relDataKey)?;

        let origin: KV = self.generateOrigin(relDataKey, meta::DATA_KEY_INVALID);

        // self.session.writeAddDataMutation(&relation.name, dataAdd, xminAdd, xmaxAdd, origin);
        self.session.writeAddDataMutation2Dest(relation.id, dataAdd, xminAdd, xmaxAdd, origin, mutationsDest);

        //--------------------------------------------------------------------

        let mut pointerKeyBuffer = self.withCapacityIn(meta::POINTER_KEY_BYTE_LEN);

        let mut process =
            |dbObjectId: DBObjectId, selfDataKey: DataKey,
             pointerKeyTag: KeyTag,
             targetTable: &Table, targetDataKey: DataKey| {
                let (xmin, xmax) = self.generateAddPointerXminXmax(&mut pointerKeyBuffer, selfDataKey, pointerKeyTag, targetTable.id, targetDataKey)?;
                //self.session.writeAddPointerMutation(&selfTable.name, xmin, xmax);
                self.session.writeAddPointerMutation2Dest(dbObjectId, xmin, xmax, mutationsDest);

                Result::<()>::Ok(())
            };

        // 对src来说
        // key + rel的tableId + rel的key
        {
            // todo 要是srcSatisfiedVec太大如何应对 挨个遍历set不现实
            // 尚未设置过滤条件 得到的是全部的
            if srcSatisfiedDatas[0].0 == global::TOTAL_DATA_OF_TABLE {
                for srcDataKey in srcSatisfiedDatas[1].0..=srcSatisfiedDatas[2].0 {
                    process(srcTable.id, srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation, relDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedDatas {
                    process(srcTable.id, *srcDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation, relDataKey)?;
                }
            }
        }

        // 对rel来说
        // key + src的tableId + src的key
        // key + dest的tableId + dest的key
        {
            // 尚未设置过滤条件 得到的是全部的
            if srcSatisfiedDatas[0].0 == global::TOTAL_DATA_OF_TABLE {
                for srcDataKey in srcSatisfiedDatas[1].0..=srcSatisfiedDatas[2].0 {
                    process(relation.id, relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable, srcDataKey)?;
                }
            } else {
                for (srcDataKey, _) in &srcSatisfiedDatas {
                    process(relation.id, relDataKey, meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable, *srcDataKey)?;
                }
            }

            if destSatisfiedDatas[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedDatas[1].0..=srcSatisfiedDatas[2].0 {
                    process(relation.id, relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable, destDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedDatas {
                    process(relation.id, relDataKey, meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable, *destDataKey)?;
                }
            }
        }

        // 对dest来说
        // key + rel的tableId + rel的key
        {
            if destSatisfiedDatas[0].0 == global::TOTAL_DATA_OF_TABLE {
                for destDataKey in srcSatisfiedDatas[1].0..=srcSatisfiedDatas[2].0 {
                    process(destTable.id, destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation, relDataKey)?;
                }
            } else {
                for (destDataKey, _) in &destSatisfiedDatas {
                    process(destTable.id, *destDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation, relDataKey)?;
                }
            }
        }

        Ok((CommandExecResult::DmlResult, Some(destSatisfiedDatas)))
    }

    /// link user(id=1 and 0=6) -usage(number = 9) -> car -own(number=1)-> tyre
    fn linkChain(&self,
                 linkTos: &[LinkTo],
                 mutationsDest: &mut SessionVec<(DBObjectId, Mutation)>) -> Result<CommandExecResult> {
        let mut lastRoundDestSatisfiedDatas = None;

        // 将 selectRel 转换成为 linkTo
        for linkTo in linkTos {
            (_, lastRoundDestSatisfiedDatas) = self.linkTo(&linkTo, lastRoundDestSatisfiedDatas, mutationsDest)?;

            // 要是中途出现了断档,之前连线要全部的废掉
            if lastRoundDestSatisfiedDatas.is_none() {
                mutationsDest.clear();
                return Ok(CommandExecResult::DmlResult);
            }
        }

        Ok(CommandExecResult::DmlResult)
    }
}