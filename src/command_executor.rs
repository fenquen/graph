use std::collections::{BTreeMap, HashMap};
use std::ops::{Index, Range, RangeFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::{byte_slice_to_u64, extract_target_data_key_from_pointer_key, extract_prefix_from_key_slice, extract_row_id_from_data_key, extract_tx_id_from_mvcc_key, extract_tx_id_from_pointer_key, global,
            key_prefix_add_row_id, meta, suffix_plus_plus, throw, u64_to_byte_array_reference, extract_row_id_from_key_slice};
use crate::meta::{Column, Table, TableType};
use crate::parser::{Command, Delete, Element, Insert, Link, Select, SelectRel, SelectTable, Unlink, UnlinkLinkStyle, UnlinkSelfStyle, Update};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use bytes::{BufMut, Bytes, BytesMut};
use rocksdb::{AsColumnFamilyRef, BoundColumnFamily, DBAccess, DBIteratorWithThreadMode,
              DBRawIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode, MultiThreaded};
use strum_macros::Display;
use crate::codec::{BinaryCodec, MyBytes};
use crate::expr::Expr;
use crate::graph_error::GraphError;
use crate::graph_value::{GraphValue, PointDesc};
use crate::session::Session;
use crate::types::{Byte, ColumnFamily, DataKey, DBIterator, DBRawIterator, KeyTag, KV, RowId, SelectResultToFront, TableId, TxId};
use crate::types;

type RowData = HashMap<String, GraphValue>;

#[macro_export]
macro_rules! JSON_ENUM_UNTAGGED {
    ($expr: expr) => {
        {
            global::UNTAGGED_ENUM_JSON.set(true);
            let r = $expr;
            global::UNTAGGED_ENUM_JSON.set(false);
            r
        }
    };
}

#[derive(Debug, Display)]
pub enum CommandExecResult {
    SelectResult(Vec<Value>),
    DmlResult,
    DdlResult,
}

pub struct CommandExecutor<'session> {
    pub session: &'session Session,
}

impl<'session> CommandExecutor<'session> {
    pub fn new(session: &'session Session) -> Self {
        CommandExecutor {
            session
        }
    }

    pub fn execute(&self, commands: &mut [Command]) -> Result<SelectResultToFront> {
        // 单个的command可能得到单个Vec<Value>
        let mut valueVecVec = Vec::with_capacity(commands.len());

        for command in commands {
            let executionResult = match command {
                Command::CreateTable(table) => {
                    let table = Table {
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        type0: table.type0.clone(),
                        rowIdCounter: AtomicU64::default(),
                        tableId: TableId::default(),
                        createIfNotExist: table.createIfNotExist,
                    };

                    self.createTable(table)?
                }
                Command::Insert(insertValues) => self.insert(insertValues)?,
                Command::Select(select) => self.select(select)?,
                Command::Link(link) => self.link(link)?,
                Command::Delete(delete) => self.delete(delete)?,
                Command::Update(update) => self.update(update)?,
                Command::Unlink(unlink) => self.unlink(unlink)?,
                _ => throw!(&format!("unsupported command {:?}", command)),
            };

            // 如何应对多个的select
            if let CommandExecResult::SelectResult(values) = executionResult {
                println!("{}\n", serde_json::to_string(&values)?);
                valueVecVec.push(values);
            }
        }

        Ok(valueVecVec)
    }

    fn createTable(&self, mut table: Table) -> Result<CommandExecResult> {
        if meta::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
            if table.createIfNotExist == false {
                throw!(&format!("table/relation: {} already exist", table.name))
            }

            return Ok(CommandExecResult::DdlResult);
        }

        table.tableId = meta::TABLE_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // 生成column family
        self.session.createColFamily(table.name.as_str())?;

        // todo 使用 u64的tableId 为key 完成
        let key = u64_to_byte_array_reference!(table.tableId);
        let json = serde_json::to_string(&table)?;
        meta::STORE.metaStore.put(key, json.as_bytes())?;

        // map
        meta::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

        Ok(CommandExecResult::DdlResult)
    }

    // todo insert时候value的排布要和创建表的时候column的顺序对应 完成
    fn insert(&self, insert: &mut Insert) -> Result<CommandExecResult> {
        // 对应的表是不是exist
        let table = self.getTableRefByName(&insert.tableName)?;

        // 不能对relation使用insert into
        if let TableType::Relation = table.type0 {
            throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insert.tableName));
        }

        let rowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let dataKey: DataKey = key_prefix_add_row_id!(meta::KEY_PREFIX_DATA, rowId);

        // 写 data本身的key和value
        let dataAdd = {
            let dataKeyBinary = u64_to_byte_array_reference!(dataKey);
            let rowDataBinary = self.generateInsertValuesBinary(insert, &*table)?;

            (dataKeyBinary.to_vec(), rowDataBinary.to_vec()) as KV
        };

        // 写 xmin xmax 对应的 mvcc key
        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, dataKey)?;

        let origin = self.generateOrigin(dataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&table.name, dataAdd, xminAdd, xmaxAdd, origin);

        Ok(CommandExecResult::DmlResult)
    }

    // todo pointer指向点和边的xmin xmax如何应对
    fn unlink(&self, unlink: &Unlink) -> Result<CommandExecResult> {
        match unlink {
            Unlink::LinkStyle(unlinkLinkStyle) => self.unlinkLinkStyle(unlinkLinkStyle),
            Unlink::SelfStyle(unlinkSelfStyle) => self.unlinkSelfStyle(unlinkSelfStyle),
        }
    }

    /// 应对 unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13) 和原来link基本相同
    /// 和原来的link套路相同是它反过来的
    fn unlinkLinkStyle(&self, unlinkLinkStyle: &UnlinkLinkStyle) -> Result<CommandExecResult> {
        let relation = self.getTableRefByName(&unlinkLinkStyle.relationName)?;
        let destTable = self.getTableRefByName(&unlinkLinkStyle.destTableName)?;
        let srcTable = self.getTableRefByName(&unlinkLinkStyle.srcTableName)?;

        let relColFamily = self.session.getColFamily(unlinkLinkStyle.relationName.as_str())?;

        // 得到rel 干掉指向src和dest的pointer key
        let relStatisfiedRowDatas = self.scanSatisfiedRows(relation.value(), unlinkLinkStyle.relationFilterExpr.as_ref(), None, true, None)?;

        // KEY_PREFIX_POINTER + relDataRowId + KEY_TAG_SRC_TABLE_ID + src的tableId + KEY_TAG_KEY
        let mut pointerKeyLeadingPartBuffer = BytesMut::with_capacity(meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET);

        // KEY_PREFIX_POINTER + relDataRowId + KEY_TAG_SRC_TABLE_ID + src的tableId + KEY_TAG_KEY + src dest rel的dataKey
        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

        let mut processRel = |statisfiedRelDataKey: DataKey, processSrc: bool| -> Result<()> {
            if processSrc {
                pointerKeyLeadingPartBuffer.writePointerKeyLeadingPart(statisfiedRelDataKey,
                                                                       meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.tableId);
            } else {
                pointerKeyLeadingPartBuffer.writePointerKeyLeadingPart(statisfiedRelDataKey,
                                                                       meta::POINTER_KEY_TAG_DEST_TABLE_ID, srcTable.tableId);
            }

            for result in self.session.getSnapshot()?.iterator_cf(&relColFamily, IteratorMode::From(pointerKeyLeadingPartBuffer.as_ref(), Direction::Forward)) {
                let (relPointerKey, _) = result?;

                // iterator是收不动尾部的要自个来弄的
                if relPointerKey.starts_with(pointerKeyLeadingPartBuffer.as_ref()) == false {
                    break;
                }

                let targetDataKey = extract_target_data_key_from_pointer_key!(&*relPointerKey);

                // 是不是符合src上的筛选expr
                if self.getRowDatasByDataKeys(&[targetDataKey], srcTable.value(), unlinkLinkStyle.srcTableFilterExpr.as_ref(), None)?.is_empty() {
                    continue;
                }

                if processSrc {
                    // 干掉src上的对应该rel的pointerKey
                    let oldXmax =
                        self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                       targetDataKey,
                                                       meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation.tableId, statisfiedRelDataKey)?;
                    self.session.writeDeletePointerMutation(&srcTable.name, oldXmax);

                    // 干掉rel上对应该src的pointerKey
                    let oldXmax =
                        self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                       statisfiedRelDataKey,
                                                       meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.tableId, targetDataKey)?;
                    self.session.writeDeletePointerMutation(&relation.name, oldXmax);
                } else {
                    // 干掉dest上的对应该rel的pointerKey
                    let oldXmax =
                        self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                       targetDataKey,
                                                       meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation.tableId, statisfiedRelDataKey)?;
                    self.session.writeDeletePointerMutation(&destTable.name, oldXmax);

                    // 干掉rel上对应该dest的pointerKey
                    let oldXmax =
                        self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                       statisfiedRelDataKey,
                                                       meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable.tableId, targetDataKey)?;
                    self.session.writeDeletePointerMutation(&relation.name, oldXmax);
                }
            }

            Ok(())
        };

        // 遍历符合要求的relRowData 得到单个上边的对应src和dest的全部dataKeys
        for (statisfiedRelDataKey, _) in relStatisfiedRowDatas {
            // src
            processRel(statisfiedRelDataKey, true)?;

            // dest
            processRel(statisfiedRelDataKey, false)?;  // 遍历符合要求的relRowData 得到单个上边的对应src和dest的全部dataKeys
        }

        Ok(CommandExecResult::DmlResult)
    }

    /// unlink user(id >1 ) as start by usage (number = 7) ,as end by own(number =7)
    /// 需要由start点出发
    fn unlinkSelfStyle(&self, unlinkSelfStyle: &UnlinkSelfStyle) -> Result<CommandExecResult> {
        Ok(CommandExecResult::DmlResult)
    }

    /// 它本质是向relation对应的data file写入
    /// 两个元素之间的relation只看种类不看里边的属性的
    fn link(&self, link: &Link) -> Result<CommandExecResult> {
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
        let relDataKey = key_prefix_add_row_id!(meta::KEY_PREFIX_DATA, relRowId);

        let dataAdd = {
            let rowDataBinary = self.generateInsertValuesBinary(&mut insertValues, &*relation)?;
            (u64_to_byte_array_reference!(relDataKey).to_vec(), rowDataBinary.to_vec()) as KV
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

    /// 如果不是含有relation的select 便是普通的select
    fn select(&self, selectFamily: &Select) -> Result<CommandExecResult> {
        match selectFamily {
            // 普通模式不含有relation
            Select::SelectTable(selectTable) => self.selectTable(selectTable),
            Select::SelectRels(selectVec) => self.selectRels(selectVec),
            _ => { panic!("undo") }
        }
    }

    /// 普通的和rdbms相同的 select
    fn selectTable(&self, selectTable: &SelectTable) -> Result<CommandExecResult> {
        let srcTable = self.getTableRefByName(selectTable.tableName.as_str())?;

        let rowDatas =
            self.scanSatisfiedRows(srcTable.value(),
                                   selectTable.tableFilterExpr.as_ref(),
                                   selectTable.selectedColNames.as_ref(),
                                   true, None)?;

        let rowDatas: Vec<RowData> = rowDatas.into_iter().map(|(_, rowData)| rowData).collect();

        let values: Vec<Value> = JSON_ENUM_UNTAGGED!(rowDatas.into_iter().map(|rowData| serde_json::to_value(&rowData).unwrap()).collect());
        // JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&rows)?));

        Ok(CommandExecResult::SelectResult(values))
    }

    /// graph特色的 rel select
    fn selectRels(&self, selectVec: &Vec<SelectRel>) -> Result<CommandExecResult> {
        // 对应1个realtion的query的多个条目的1个
        #[derive(Debug)]
        struct SelectResult {
            srcName: String,
            srcRowDatas: Vec<(DataKey, RowData)>,
            relationName: String,
            relationData: RowData,
            destName: String,
            destRowDatas: Vec<(DataKey, RowData)>,
        }

        // 给next轮用的
        let mut destDataKeysInPrevSelect: Option<Vec<DataKey>> = None;

        // 1个select对应Vec<SelectResult> 多个select对应Vec<Vec<SelectResult>>
        let mut selectResultVecVec: Vec<Vec<SelectResult>> = Vec::with_capacity(selectVec.len());

        let mut pointerKeyPrefixBuffer = BytesMut::with_capacity(meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET);

        'loopSelectVec:
        for select in selectVec {
            // 为什么要使用{} 不然的话有概率死锁 https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
            let relationDatas: Vec<(DataKey, RowData)> = {
                let relation = self.getTableRefByName(select.relationName.as_ref().unwrap())?;
                let relationDatas =
                    self.scanSatisfiedRows(relation.value(),
                                           select.relationFliterExpr.as_ref(),
                                           select.relationColumnNames.as_ref(),
                                           true, None)?;
                relationDatas.into_iter().map(|(dataKey, rowData)| (dataKey, rowData)).collect()
            };

            let mut selectResultVecInCurrentSelect = Vec::with_capacity(relationDatas.len());

            // 融合了当前的select的relationDatas的全部的dest的dataKey
            let mut destKeysInCurrentSelect = vec![];

            let srcTable = self.getTableRefByName(&select.srcTableName)?;
            // let srcColFamily = self.session.getColFamily(select.srcTableName.as_str())?;
            // let mut rawIteratorSrc = self.session.dataStore.raw_iterator_cf(&srcColFamily);

            let destTable = self.getTableRefByName(select.destTableName.as_ref().unwrap())?;
            // let destColFamily = self.session.getColFamily(select.destTableName.as_ref().unwrap())?;
            // let mut rawIteratorDest = self.session.dataStore.raw_iterator_cf(&destColFamily);

            let relColFamily = self.session.getColFamily(select.relationName.as_ref().unwrap())?;
            let tableName_mutationsOnTable = self.session.tableName_mutationsOnTable.borrow();
            let mutationsRawOnTableCurrentTx = tableName_mutationsOnTable.get(select.relationName.as_ref().unwrap());

            // 遍历当前的select的多个relation
            'loopRelationData:
            for (relationDataKey, relationData) in relationDatas {
                let mut gatherTargetDataKeys =
                    |keyTag: KeyTag, targetTable: &Table| {
                        pointerKeyPrefixBuffer.writePointerKeyLeadingPart(relationDataKey, keyTag, targetTable.tableId);

                        // todo selectRels时候如何应对pointerKey的mvcc
                        let mut pointerKeys =
                            self.getKeysByPrefix(select.relationName.as_ref().unwrap(),
                                                 &relColFamily,
                                                 pointerKeyPrefixBuffer.as_ref(),
                                                 Some(CommandExecutor::checkCommittedPointerVisibilityWithoutCurrentTxMutations),
                                                 Some(CommandExecutor::checkCommittedPointerVisibilityWithCurrentTxMutations))?;


                        // todo 应对当前tx上 add的 当前rel 当前targetTable pointer
                        if let Some(mutationsRawOnTableCurrentTx) = mutationsRawOnTableCurrentTx {
                            let addedPointerUnderRelTargetTableCurrentTxRange =
                                mutationsRawOnTableCurrentTx.range::<Vec<Byte>, RangeFrom<&Vec<Byte>>>(&pointerKeyPrefixBuffer.to_vec()..);

                            for (addedPointerUnderRelTargetTableCurrentTxRange, _) in addedPointerUnderRelTargetTableCurrentTxRange {
                                // 因为右边的是未限制的 需要手动
                                if addedPointerUnderRelTargetTableCurrentTxRange.starts_with(pointerKeyPrefixBuffer.as_ref()) == false {
                                    break;
                                }

                                if self.checkUncommittedPointerVisibility(&mutationsRawOnTableCurrentTx, &mut pointerKeyPrefixBuffer, addedPointerUnderRelTargetTableCurrentTxRange)? {
                                    pointerKeys.push(addedPointerUnderRelTargetTableCurrentTxRange.clone().into_boxed_slice());
                                }
                            }
                        }

                        let targetDataKeys = pointerKeys.into_iter().map(|pointerKey| extract_target_data_key_from_pointer_key!(&*pointerKey)).collect::<Vec<DataKey>>();

                        // todo 不知道要不要dedup

                        anyhow::Result::<Vec<DataKey>>::Ok(targetDataKeys)
                    };

                // 收罗该rel上的全部的src的dataKey
                let srcDataKeys = {
                    let srcDataKeys = gatherTargetDataKeys(meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.value())?;
                    if srcDataKeys.is_empty() {
                        continue;
                    }
                    srcDataKeys
                };

                // 收罗该rel上的全部的dest的dataKey
                let destDataKeys = {
                    let destDataKeys = gatherTargetDataKeys(meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable.value())?;
                    if destDataKeys.is_empty() {
                        continue;
                    }
                    destDataKeys
                };

                let srcRowDatas = {
                    // 上轮的全部的多个条目里边的dest的position 和 当前条目的src的position的交集
                    match destDataKeysInPrevSelect {
                        Some(ref destPositionsInPrevSelect) => {
                            let intersectDataKeys =
                                destPositionsInPrevSelect.iter().filter(|&&destDataKeyInPrevSelect| srcDataKeys.contains(&destDataKeyInPrevSelect)).map(|destDataKey| *destDataKey).collect::<Vec<_>>();

                            // 说明 当前的这个relation的src和上轮的dest没有重合的
                            if intersectDataKeys.is_empty() {
                                continue 'loopRelationData;
                            }

                            // 当前的select的src确定了 还要回去修改上轮的dest
                            if let Some(prevSelectResultVec) = selectResultVecVec.last_mut() {

                                // 遍历上轮的各个result的dest,把intersect之外的去掉
                                for prevSelectResult in &mut *prevSelectResultVec {
                                    // https://blog.csdn.net/u011528645/article/details/123117829
                                    prevSelectResult.destRowDatas.retain(|(dataKey, _)| intersectDataKeys.contains(dataKey));
                                }

                                // destRowDatas是空的话那么把selectResult去掉
                                prevSelectResultVec.retain(|prevSelectResult| prevSelectResult.destRowDatas.len() > 0);

                                // 连线断掉
                                if prevSelectResultVec.is_empty() {
                                    break 'loopSelectVec;
                                }
                            }

                            // 当前的使用intersect为源头
                            self.getRowDatasByDataKeys(&intersectDataKeys, &*srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref())?
                        }
                        // 只会在首轮的
                        None => self.getRowDatasByDataKeys(&srcDataKeys, &*srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref())?,
                    }
                };
                if srcRowDatas.is_empty() {
                    continue;
                }

                let destRowDatas = {
                    let destTable = self.getTableRefByName(select.destTableName.as_ref().unwrap())?;
                    self.getRowDatasByDataKeys(&destDataKeys, &*destTable, select.destFilterExpr.as_ref(), select.destColumnNames.as_ref())?
                };
                if destRowDatas.is_empty() {
                    continue;
                }

                for destPosition in &destDataKeys {
                    destKeysInCurrentSelect.push(*destPosition);
                }

                selectResultVecInCurrentSelect.push(
                    SelectResult {
                        srcName: select.srcAlias.as_ref().unwrap_or_else(|| &select.srcTableName).to_string(),
                        srcRowDatas,
                        relationName: select.relationAlias.as_ref().unwrap_or_else(|| select.relationName.as_ref().unwrap()).to_string(),
                        relationData,
                        destName: select.destAlias.as_ref().unwrap_or_else(|| select.destTableName.as_ref().unwrap()).to_string(),
                        destRowDatas,
                    }
                );
            }

            destDataKeysInPrevSelect = {
                // 当前的relation select 的多个realtion对应dest全都是empty的
                if destKeysInCurrentSelect.is_empty() {
                    break 'loopSelectVec;
                }

                // rust的这个去重有点不同只能去掉连续的重复的 故而需要先排序让重复的连续起来
                destKeysInCurrentSelect.sort();
                destKeysInCurrentSelect.dedup();

                Some(destKeysInCurrentSelect)
            };

            selectResultVecVec.push(selectResultVecInCurrentSelect);
        }

        /// ```[[[第1个select的第1行data],[第1个select的第2行data]],[[第2个select的第1行data],[第2个select的第2行data]]]```
        /// 到时候要生成4条脉络
        fn handleResult(selectResultVecVec: Vec<Vec<SelectResult>>) -> Vec<Value> {
            let mut valueVec = Vec::default();

            if selectResultVecVec.is_empty() {
                return valueVec;
            }

            for selectResult in &selectResultVecVec[0] {
                let mut json = json!({});

                // 把tuple的position干掉
                let srcRowDatas: Vec<&RowData> = selectResult.srcRowDatas.iter().map(|(_, rownData)| rownData).collect();
                let destRowDatas: Vec<&RowData> = selectResult.destRowDatas.iter().map(|(_, rowData)| rowData).collect();

                // 把map的src和dest干掉
                let relationData: HashMap<&String, &GraphValue> = selectResult.relationData.iter().filter(|&pair| pair.0 != PointDesc::SRC && pair.0 != PointDesc::DEST).collect();

                // 对json::Value来说需要注意的是serialize的调用发生在这边 而不是serde_json::to_string()
                json[selectResult.srcName.as_str()] = json!(srcRowDatas);
                json[selectResult.relationName.as_str()] = json!(relationData);
                json[selectResult.destName.as_str()] = json!(destRowDatas);

                let mut selectVecResultVecVecIndex = 1usize;
                loop {
                    // 到下个select的维度上
                    let outerIndex = suffix_plus_plus!(selectVecResultVecVecIndex);
                    if outerIndex == selectResultVecVec.len() {
                        break;
                    }

                    for selectResult in selectResultVecVec.get(outerIndex).unwrap() {
                        json[selectResult.relationName.as_str()] = json!(selectResult.relationData);

                        let destRowDatas: Vec<&RowData> = selectResult.destRowDatas.iter().map(|(_, rowData)| rowData).collect();
                        json[selectResult.destName.as_str()] = json!(destRowDatas);
                    }
                }

                valueVec.push(json);
            }

            valueVec
        }

        let valueVec = JSON_ENUM_UNTAGGED!(handleResult(selectResultVecVec));
        //println!("{}", serde_json::to_string(&valueVec)?);

        Ok(CommandExecResult::SelectResult(valueVec))
    }

    // todo 要是point还有rel的联系不能update 完成
    fn update(&self, update: &Update) -> Result<CommandExecResult> {
        let table = self.getTableRefByName(update.tableName.as_str())?;

        if let TableType::Relation = table.type0 {
            throw!("can not use update on relation");
        }

        let columnName_column = {
            let mut columnName_column = HashMap::with_capacity(table.columns.len());
            for column in &table.columns {
                columnName_column.insert(column.name.to_string(), column.clone());
            }

            columnName_column
        };

        // 要是data有link的话 通过抛异常来跳出scanSatisfiedRows的循环
        let testDataHasBeenLinked = |commandExecutor: &CommandExecutor, columnFamily: &ColumnFamily, dataKey: DataKey, rowDataBinary: &[Byte]| {
            let rowId = extract_row_id_from_data_key!(dataKey);
            let pointerKeyPrefix = u64_to_byte_array_reference!(key_prefix_add_row_id!(meta::KEY_PREFIX_POINTER, rowId));

            let mut dbIterator: DBIterator = commandExecutor.session.getSnapshot()?.iterator_cf(columnFamily, IteratorMode::From(pointerKeyPrefix, Direction::Forward));
            if let Some(kv) = dbIterator.next() {
                let (pointerKey, _) = kv?;
                // 说明有该data条目对应的pointerKey
                if rowId == extract_row_id_from_key_slice!(pointerKey) {
                    throw!("update can not execute, because data has been linked");
                }
            }

            anyhow::Result::<bool>::Ok(true)
        };

        let mut pairs = self.scanSatisfiedRows(table.value(), update.filterExpr.as_ref(), None, true, Some(testDataHasBeenLinked))?;

        enum A<'a> {
            DirectValue(GraphValue),
            NeedCalc(&'a Expr),
        }

        let mut columnName_a: HashMap<String, A> = HashMap::with_capacity(update.columnName_expr.len());

        let compatibleCheck = |columnName: &String, columnValue: &GraphValue| {
            match columnName_column.get(columnName) {
                Some(column) => {
                    if column.type0.compatible(columnValue) == false {
                        throw!(&format!("table:{} , column:{}, is not compatilbe with value:{:?}", update.tableName, columnName, columnValue));
                    }
                }
                None => throw!(&format!("table:{} has no column named:{}", update.tableName, columnName)),
            }

            anyhow::Result::<(), GraphError>::Ok(())
        };

        // todo logical优化
        // column expr能直接计算的先计算 不要到后边的遍历里边重复计算了
        for (columnName, columnExpr) in &update.columnName_expr {
            if columnExpr.needAcutalRowData() {
                columnName_a.insert(columnName.to_string(), A::NeedCalc(columnExpr));
            } else {
                let columnValue = columnExpr.calc(None)?;

                // update设置的值要和column type 相同
                compatibleCheck(columnName, &columnValue)?;

                columnName_a.insert(columnName.to_string(), A::DirectValue(columnValue));
            }
        }

        // todo update的时候能不能直接从binary维度上更改row
        for (dataKey, rowData) in &mut pairs {
            for (columnName, a) in &columnName_a {
                match a {
                    A::NeedCalc(expr) => {
                        let columnValue = expr.calc(Some(rowData))?;

                        // update设置的值要和column type 相同
                        compatibleCheck(columnName, &columnValue)?;

                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                    A::DirectValue(columnValue) => {
                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                }
            }

            // todo 各个column的value都在1道使得update时候只能整体来弄太耦合了 后续设想能不能各个column保存到单独的key

            let value = {
                let mut destByteSlice = BytesMut::new();
                // 需要以表定义里边的column顺序来序列化
                for column in &table.columns {
                    let columnValue = rowData.get(&column.name).unwrap();
                    columnValue.encode(&mut destByteSlice)?;
                }

                destByteSlice.freeze()
            };
            let value = value.as_ref();

            // 写老的data的xmax
            let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
            let oldXmax = self.generateDeleteDataXmax(&mut mvccKeyBuffer, *dataKey)?;

            // 写新的data
            let newRowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
            let newDataKey = key_prefix_add_row_id!(meta::KEY_PREFIX_MVCC,newRowId);
            let newData: KV = (u64_to_byte_array_reference!(newDataKey).to_vec(), value.to_vec());

            // 写新的data的xmin xmax
            let (newXmin, newXmax) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, newDataKey)?;

            let origin = self.generateOrigin(newDataKey, *dataKey);

            self.session.writeUpdateDataMutation(&table.name, oldXmax, newData, newXmin, newXmax, origin);
        }

        Ok(CommandExecResult::DmlResult)
    }

    // todo rel能不能直接delete 应该先把rel上的点全都取消 rel不存在src和dest的点 然后
    /// 得到满足expr的record 然后把它的xmax变为当前的txId
    fn delete(&self, delete: &Delete) -> Result<CommandExecResult> {
        let pairs = {
            let table = self.getTableRefByName(delete.tableName.as_str())?;
            self.scanSatisfiedRows(table.value(), delete.filterExpr.as_ref(), None, true, None)?
        };

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);

        // 遍历添加当前tx对应的xmax
        for (dataKey, _) in pairs {
            let oldXmax = self.generateDeleteDataXmax(&mut mvccKeyBuffer, dataKey)?;
            self.session.writeDeleteDataMutation(&delete.tableName, oldXmax);
        }

        Ok(CommandExecResult::DmlResult)
    }

    /// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
    fn getRowDatasByDataKeys(&self,
                             dataKeys: &[DataKey],
                             table: &Table,
                             tableFilter: Option<&Expr>,
                             selectedColNames: Option<&Vec<String>>) -> Result<Vec<(DataKey, RowData)>> {
        let mut rowDatas = Vec::with_capacity(dataKeys.len());

        let columnFamily = self.session.getColFamily(&table.name)?;

        let mut mvccKeyBuffer = &mut BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);

        let tableName_mutationsOnTable = self.session.tableName_mutationsOnTable.borrow();
        let mutationsRawCurrentTx = tableName_mutationsOnTable.get(&table.name);

        let mut process =
            |dataKey: DataKey| -> Result<()> {
                // todo getRowDatasByDataKeys() 也要mvcc筛选 完成
                // mvcc的visibility筛选
                if self.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer, &mut rawIterator, dataKey, &columnFamily)? == false {
                    return Ok(());
                }

                if let Some(mutationsRawCurrentTx) = mutationsRawCurrentTx {
                    if self.checkCommittedDataVisibilityWithTxMutations(mutationsRawCurrentTx, &mut mvccKeyBuffer, dataKey)? == false {
                        return Ok(());
                    }
                }

                let rowDataBinary =
                    match self.session.getSnapshot()?.get_cf(&columnFamily, u64_to_byte_array_reference!(dataKey))? {
                        Some(rowDataBinary) => rowDataBinary,
                        None => return Ok(()), // 有可能
                    };

                if let Some(rowData) = self.readRowDataBinary(table, rowDataBinary.as_slice(), tableFilter, selectedColNames)? {
                    rowDatas.push((dataKey, rowData));
                }

                Ok(())
            };

        // 要得到表的全部的data
        if dataKeys[0] == global::TOTAL_DATA_OF_TABLE {
            for dataKey in dataKeys[1]..=dataKeys[2] {
                process(dataKey)?;
            }
        } else {
            for dataKey in dataKeys {
                process(*dataKey)?;
            }
        }

        Ok(rowDatas)
    }

    // todo 实现 index
    fn scanSatisfiedRows(&self, table: &Table,
                         tableFilter: Option<&Expr>,
                         selectedColumnNames: Option<&Vec<String>>,
                         select: bool,
                         rowChecker: Option<fn(commandExecutor: &CommandExecutor,
                                               columnFamily: &ColumnFamily,
                                               dataKey: DataKey,
                                               rowDataBinary: &[Byte]) -> Result<bool>>) -> Result<Vec<(DataKey, RowData)>> {
        // todo 使用table id 为 column family 标识
        let columnFamily = self.session.getColFamily(&table.name)?;

        let tableName_mutationsOnTable = self.session.tableName_mutationsOnTable.borrow();
        let mutationsRawOnTableCurrentTx = tableName_mutationsOnTable.get(&table.name);

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);

        let mut satisfiedRows =
            if tableFilter.is_some() || select {
                let mut satisfiedRows = Vec::new();

                // mvcc的visibility筛选
                let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);

                // todo scan遍历能不能concurrent
                // 对data条目而不是pointer条目遍历
                for iterResult in self.session.getSnapshot()?.iterator_cf(&columnFamily, IteratorMode::From(meta::DATA_KEY_PATTERN, Direction::Forward)) {
                    let (dataKeyBinary, rowDataBinary) = iterResult?;

                    // prefix iterator原理只是seek到prefix对应的key而已 到后边可能会超过范围 https://www.jianshu.com/p/9848a376d41d
                    // 前4个bit的值是不是 KEY_PREFIX_DATA
                    if extract_prefix_from_key_slice!(dataKeyBinary) != meta::KEY_PREFIX_DATA {
                        break;
                    }

                    let dataKey = byte_slice_to_u64!(&*dataKeyBinary) as DataKey;

                    if let Some(ref rowChecker) = rowChecker {
                        if rowChecker(self, &columnFamily, dataKey, &*rowDataBinary)? == false {
                            continue;
                        }
                    }

                    // mvcc的visibility筛选
                    if self.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer, &mut rawIterator, dataKey, &columnFamily)? == false {
                        continue;
                    }

                    // 以上是全都在已落地的维度内的visibility check 还要结合当前事务上的尚未提交的mutations
                    // 先要结合mutations 看已落地的是不是应该干掉
                    // 然后看mutations 有没有想要的
                    if let Some(mutationsRawCurrentTx) = mutationsRawOnTableCurrentTx {
                        if self.checkCommittedDataVisibilityWithTxMutations(mutationsRawCurrentTx, &mut mvccKeyBuffer, dataKey)? == false {
                            continue;
                        }
                    }

                    // mvcc筛选过了 对rowData本身的筛选
                    if let Some(rowData) = self.readRowDataBinary(table, &*rowDataBinary, tableFilter, selectedColumnNames)? {
                        satisfiedRows.push((dataKey, rowData));
                    }
                }

                satisfiedRows
            } else { // 说明是link 且尚未写filterExpr
                let mut rawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily) as DBRawIterator;
                rawIterator.seek(meta::DATA_KEY_PATTERN);

                if rawIterator.valid() == false {
                    vec![]
                } else {
                    // start include
                    let startKeyBinInclude = rawIterator.key().unwrap();
                    let startKeyInclude = byte_slice_to_u64!(startKeyBinInclude);

                    // end include
                    // seek_for_prev 意思是 定位到目标 要是目标没有的话 那么定位到它前个
                    rawIterator.seek_for_prev(u64_to_byte_array_reference!(((meta::KEY_PREFIX_DATA + 1) as u64)  << meta::ROW_ID_BIT_LEN));
                    let endKeyBinInclude = rawIterator.key().unwrap();
                    let endKeyInclude = byte_slice_to_u64!(endKeyBinInclude);

                    // todo 可能要应对后续的肯能的rowId回收
                    vec![
                        (global::TOTAL_DATA_OF_TABLE, HashMap::default()),
                        (startKeyInclude, HashMap::default()),
                        (endKeyInclude, HashMap::default()),
                    ]
                }
            };

        // todo scan的时候要是当前tx有add的话 也要收录 完成
        if let Some(mutationsRawOnTableCurrentTx) = mutationsRawOnTableCurrentTx {
            let addedDataCurrentTxRange = mutationsRawOnTableCurrentTx.range::<Vec<Byte>, Range<&Vec<Byte>>>(&*meta::DATA_KEY_PATTERN_VEC..&*meta::POINTER_KEY_PATTERN_VEC);

            for (addedDataKeyBinaryCurrentTx, addRowDataBinaryCurrentTx) in addedDataCurrentTxRange {
                let addedDataKeyCurrentTx: DataKey = byte_slice_to_u64!(addedDataKeyBinaryCurrentTx);

                if self.checkUncommittedDataVisibility(mutationsRawOnTableCurrentTx, &mut mvccKeyBuffer, addedDataKeyCurrentTx)? == false {
                    continue;
                }

                if let Some(rowData) = self.readRowDataBinary(table, addRowDataBinaryCurrentTx, tableFilter, selectedColumnNames)? {
                    satisfiedRows.push((addedDataKeyCurrentTx, rowData));
                }
            }
        }

        Ok(satisfiedRows)
    }

    //-------------------------------------------------------------------------------------------

    // 对data对应的mvccKey的visibility筛选
    fn checkCommittedDataVisibilityWithoutTxMutations(&self,
                                                      mvccKeyBuffer: &mut BytesMut,
                                                      rawIterator: &mut DBRawIterator,
                                                      dataKey: DataKey,
                                                      columnFamily: &ColumnFamily) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // xmin
        // 当vaccum时候会变为 TX_ID_FROZEN 别的时候不会变动 只会有1条
        mvccKeyBuffer.writeDataMvccXmin(dataKey, meta::TX_ID_FROZEN);
        rawIterator.seek(mvccKeyBuffer.as_ref());
        // rawIterator生成的时候可以通过readOption设置bound 要是越过的话iterator.valid()为false
        let mvccKeyXmin = rawIterator.key().unwrap();
        let xmin = extract_tx_id_from_mvcc_key!(mvccKeyXmin);

        // xmax
        mvccKeyBuffer.writeDataMvccXmax(dataKey, currentTxId);
        rawIterator.seek_for_prev(mvccKeyBuffer.as_ref());
        // 能确保会至少有xmax是0的 mvcc条目
        let mvccKeyXmax = rawIterator.key().unwrap();
        let xmax = extract_tx_id_from_mvcc_key!(mvccKeyXmax);

        let snapshot = self.session.getSnapshot()?;

        let originDataKeyKey = u64_to_byte_array_reference!(key_prefix_add_row_id!(meta::KEY_PPREFIX_ORIGIN_DATA_KEY, extract_row_id_from_data_key!(dataKey)));
        let originDataKey = snapshot.get_cf(columnFamily, originDataKeyKey)?.unwrap();
        let originDataKey = byte_slice_to_u64!(originDataKey);
        // 说明本条data是通过update而来 老data的dataKey是originDataKey
        if meta::DATA_KEY_INVALID != originDataKey {
            // 探寻originDataKey对应的mvcc xmax记录
            mvccKeyBuffer.writeDataMvccXmax(originDataKey, currentTxId);
            rawIterator.seek_for_prev(mvccKeyBuffer.as_ref());

            // 能确保会至少有xmax是0的 mvcc条目
            // 得知本tx可视范围内该条老data是recently被哪个tx干掉的
            let originDataXmax = extract_tx_id_from_mvcc_key!( rawIterator.key().unwrap());
            // 要和本条data的xmin比较 如果不相等的话抛弃
            if xmin != originDataXmax {
                return Ok(false);
            }
        }

        Ok(meta::isVisible(currentTxId, xmin, xmax))
    }

    fn checkCommittedDataVisibilityWithTxMutations(&self,
                                                   mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                   mvccKeyBuffer: &mut BytesMut,
                                                   dataKey: DataKey) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要看落地的有没有被当前的tx上的干掉  只要读取相应的xmax的mvccKey
        // mutationsRawCurrentTx的txId只会是currentTxId
        mvccKeyBuffer.writeDataMvccXmax(dataKey, currentTxId);

        Ok(mutationsRawCurrentTx.get(mvccKeyBuffer.as_ref()).is_none())
    }

    fn checkUncommittedDataVisibility(&self,
                                      mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                      mvccKeyBuffer: &mut BytesMut,
                                      addedDataKeyCurrentTx: DataKey) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 检验当前tx上新add的话 只要检验相应的xmax便可以了 就算有xmax那对应的txId也只会是currentTx
        mvccKeyBuffer.writeDataMvccXmax(addedDataKeyCurrentTx, currentTxId);

        // 说明这个当前tx上insert的data 后来又被当前tx的干掉了
        Ok(mutationsRawCurrentTx.get(mvccKeyBuffer.as_ref()).is_none())
    }

    // todo  pointerKey如何应对mvcc 完成
    /// 因为mvcc信息直接是在pointerKey上的 去看它的末尾的xmax
    fn checkCommittedPointerVisibilityWithoutCurrentTxMutations(&self,
                                                                pointerKeyBuffer: &mut BytesMut,
                                                                rawIterator: &mut DBRawIterator,
                                                                committedPointerKey: &[Byte]) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        const RANGE: Range<usize> = meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET..meta::POINTER_KEY_BYTE_LEN;

        // pointerKey末尾的 mvccKeyTag和txId
        let pointerKeyTail = committedPointerKey.index(RANGE);

        // 读取 mvccKeyTag
        match *pointerKeyTail.first().unwrap() {
            // 含有xmin的pointerKey 抛弃掉不要,是没有问题的因为相应的指向信息在xmax的pointerKey也有
            meta::MVCC_KEY_TAG_XMIN => Ok(false),
            meta::MVCC_KEY_TAG_XMAX => {
                let xmax = byte_slice_to_u64!(&pointerKeyTail[1..]) as TxId;

                if currentTxId >= xmax {
                    if (xmax == meta::TX_ID_INVALID) == false {
                        return Ok(false);
                    }
                }

                // 到了这边说明 满足 xmax == 0 || xmax > currentTxId
                // 到了这边说明该pointerKey本身单独看是没有问题的 不过还需要联系后边是不是会干掉
                // 要是后边还有 currentTxId > xmax 的 就需要应对
                let prefix = &committedPointerKey[..meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET];

                // 生成xmax是currentTxId 的pointerKey
                pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMIN, meta::TX_ID_FROZEN);
                rawIterator.seek(pointerKeyBuffer.as_ref());
                let xmin = extract_tx_id_from_pointer_key!(rawIterator.key().unwrap());

                pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMAX, currentTxId);
                rawIterator.seek_for_prev(pointerKeyBuffer.as_ref());
                let xmax = extract_tx_id_from_pointer_key!(rawIterator.key().unwrap());

                Ok(meta::isVisible(currentTxId, xmin, xmax))
            }
            _ => panic!("impossible")
        }
    }

    fn checkCommittedPointerVisibilityWithCurrentTxMutations(&self,
                                                             mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                             pointerKeyBuffer: &mut BytesMut,
                                                             committedPointerKey: &[Byte]) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要是当前的tx干掉的话会有这样的xmax
        pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMAX, currentTxId);

        Ok(mutationsRawCurrentTx.get(pointerKeyBuffer.as_ref()).is_none())
    }

    fn checkUncommittedPointerVisibility(&self,
                                         mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                         pointerKeyBuffer: &mut BytesMut,
                                         addedPointerKeyCurrentTx: &[Byte]) -> Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要是当前的tx干掉的话会有这样的xmax
        pointerKeyBuffer.replacePointerKeyMcvvTagTxId(addedPointerKeyCurrentTx, meta::MVCC_KEY_TAG_XMAX, currentTxId);

        Ok(mutationsRawCurrentTx.get(pointerKeyBuffer.as_ref()).is_none())
    }

    // ----------------------------------------------------------------------------------------

    fn readRowDataBinary(&self,
                         table: &Table,
                         rowBinary: &[u8],
                         tableFilterExpr: Option<&Expr>,
                         selectedColumnNames: Option<&Vec<String>>) -> Result<Option<RowData>> {
        let columnNames = table.columns.iter().map(|column| column.name.clone()).collect::<Vec<String>>();

        // todo 如何不去的copy
        let mut myBytesRowData = MyBytes::from(Bytes::from(Vec::from(rowBinary)));
        let columnValues = Vec::try_from(&mut myBytesRowData)?;

        if columnNames.len() != columnValues.len() {
            throw!("column names count does not match column values");
        }

        let mut rowData: RowData = HashMap::with_capacity(columnNames.len());

        for columnName_columnValue in columnNames.into_iter().zip(columnValues) {
            rowData.insert(columnName_columnValue.0, columnName_columnValue.1);
        }

        let rowData =
            if selectedColumnNames.is_some() {
                let mut a = HashMap::with_capacity(rowData.len());

                for selectedColumnName in selectedColumnNames.unwrap() {
                    let entry = rowData.remove_entry(selectedColumnName);

                    // 说明指明的column不存在
                    if entry.is_none() {
                        throw!(&format!("not have column:{}", selectedColumnName));
                    }

                    let entry = entry.unwrap();

                    a.insert(entry.0, entry.1);
                }

                a
            } else {
                rowData
            };

        if tableFilterExpr.is_none() {
            return Ok(Some(rowData));
        }

        if let GraphValue::Boolean(satisfy) = tableFilterExpr.unwrap().calc(Some(&rowData))? {
            if satisfy {
                Ok(Some(rowData))
            } else {
                Ok(None)
            }
        } else {
            throw!("table filter should get a boolean")
        }
    }

    fn getTableRefMutByName(&self, tableName: &str) -> Result<RefMut<String, Table>> {
        let table = meta::TABLE_NAME_TABLE.get_mut(tableName);
        if table.is_none() {
            throw!(&format!("table:{} not exist", tableName));
        }

        Ok(table.unwrap())
    }

    fn getTableRefByName(&self, tableName: &str) -> Result<Ref<String, Table>> {
        let table = meta::TABLE_NAME_TABLE.get(tableName);
        if table.is_none() {
            throw!(&format!("table:{} not exist", tableName));
        }
        Ok(table.unwrap())
    }

    fn generateInsertValuesBinary(&self, insert: &mut Insert, table: &Table) -> Result<Bytes> {
        // 要是未显式说明column的话还需要读取table的column
        if insert.useExplicitColumnNames == false {
            for column in &table.columns {
                insert.columnNames.push(column.name.clone());
            }
        } else { // 如果显式说明columnName的话需要确保都是有的
            for columnNameToInsert in &insert.columnNames {
                let mut found = false;

                for column in &table.columns {
                    if columnNameToInsert == &column.name {
                        found = true;
                        break;
                    }
                }

                if found == false {
                    throw!(&format!("column {} does not defined", columnNameToInsert));
                }
            }

            // 说明column未写全 需要确认absent的是不是都是nullable
            if insert.columnNames.len() != table.columns.len() {
                let columnNames = insert.columnNames.clone();
                let absentColumns: Vec<&Column> = collectionMinus0(&table.columns, &columnNames, |column, columnName| { &column.name == columnName });
                for absentColumn in absentColumns {
                    if absentColumn.nullable {
                        insert.columnNames.push(absentColumn.name.clone());
                        insert.columnExprs.push(Expr::Single(Element::Null));
                    } else {
                        throw!(&format!("table:{}, column:{} is not nullable", table.name, absentColumn.name));
                    }
                }
            }
        }

        // todo insert时候需要各column全都insert 后续要能支持 null的 GraphValue 完成
        // 确保column数量和value数量相同
        if insert.columnNames.len() != insert.columnExprs.len() {
            throw!("column count does not match value count");
        }

        /// 取差集
        fn collectionMinus<'a, T: Clone + PartialEq>(collectionA: &'a [T], collectionB: &'a [&'a T]) -> Vec<&'a T> {
            collectionA.iter().filter(|u| !collectionB.contains(u)).collect::<Vec<&'a T>>()
        }

        fn collectionMinus0<'a, T, T0>(collectionT: &'a [T],
                                       collectionT0: &'a [T0],
                                       tEqT0: impl Fn(&T, &T0) -> bool) -> Vec<&'a T> where T: Clone + PartialEq,
                                                                                            T0: Clone + PartialEq {
            let mut a = vec![];

            for t in collectionT {
                for t0 in collectionT0 {
                    if tEqT0(t, t0) == false {
                        a.push(t);
                    }
                }
            }

            a
        }

        // todo 如果指明了要insert的column name的话 需要排序 符合表定义时候的column顺序 完成
        let destByteSlice = {
            let mut columnName_columnExpr = HashMap::with_capacity(insert.columnNames.len());
            for (columnName, columnExpr) in insert.columnNames.iter().zip(insert.columnExprs.iter()) {
                columnName_columnExpr.insert(columnName, columnExpr);
            }

            let mut destByteSlice = BytesMut::new();

            // 要以create时候的顺序encode
            for column in &table.columns {
                let columnExpr = columnName_columnExpr.get(&column.name).unwrap();

                // columnType和value要对上
                let columnValue = columnExpr.calc(None)?;
                if column.type0.compatible(&columnValue) == false {
                    throw!(&format!("column:{},type:{} is not compatible with value:{}", column.name, column.type0, columnValue));
                }

                columnValue.encode(&mut destByteSlice)?;
            }

            destByteSlice
        };

        Ok(destByteSlice.freeze())
    }

    fn getKeysByPrefix(&self,
                       tableName: &str,
                       colFamily: &impl AsColumnFamilyRef,
                       prefix: &[Byte],
                       // 和上下文有联系的闭包不能使用fn来表示 要使用Fn的tarit表示 fn是函数指针只和入参有联系 它可以用Fn的trait表达
                       filterWithoutMutation: Option<fn(&CommandExecutor<'session>,
                                                        pointerKeyBuffer: &mut BytesMut,
                                                        rawIterator: &mut DBRawIterator,
                                                        pointerKey: &[Byte]) -> Result<bool>>,
                       filterWithMutation: Option<fn(&CommandExecutor<'session>,
                                                     mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                     pointerKeyBuffer: &mut BytesMut,
                                                     pointerKey: &[Byte]) -> Result<bool>>) -> Result<Vec<Box<[Byte]>>> {
        let mut keys = Vec::new();

        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);
        let mut rawIterator = self.session.getSnapshot()?.raw_iterator_cf(colFamily) as DBRawIterator;

        let tableName_mutationsOnTable = self.session.tableName_mutationsOnTable.borrow();
        let mutationsRawCurrentTx = tableName_mutationsOnTable.get(tableName);

        for iterResult in self.session.getSnapshot()?.iterator_cf(colFamily, IteratorMode::From(prefix, Direction::Forward)) {
            let (key, _) = iterResult?;

            // 说明越过了
            if key.starts_with(prefix) == false {
                break;
            }

            if let Some(filterWithoutMutation) = filterWithoutMutation.as_ref() {
                if filterWithoutMutation(self, &mut pointerKeyBuffer, &mut rawIterator, key.as_ref())? == false {
                    continue;
                }
            }

            if let Some(filterWithMutation) = filterWithMutation.as_ref() {
                if let Some(mutationsRawCurrentTx) = mutationsRawCurrentTx {
                    if filterWithMutation(self, mutationsRawCurrentTx, &mut pointerKeyBuffer, key.as_ref())? == false {
                        continue;
                    }
                }
            }

            keys.push(key);
        }

        Ok(keys)
    }

    /// 当前tx上add时候生成 xmin xmax 对应的mvcc key
    fn generateAddDataXminXmax(&self, mvccKeyBuffer: &mut BytesMut, dataKey: DataKey) -> Result<(KV, KV)> {
        let xmin = {
            mvccKeyBuffer.writeDataMvccXmin(dataKey, self.session.getTxId()?);
            (mvccKeyBuffer.to_vec(), global::EMPTY_BINARY)
        };

        let xmax = {
            mvccKeyBuffer.writeDataMvccXmax(dataKey, meta::TX_ID_INVALID);
            (mvccKeyBuffer.to_vec(), global::EMPTY_BINARY)
        };

        Ok((xmin, xmax))
    }

    /// 当前tx上delete时候生成 xmax的 mvccKey
    fn generateDeleteDataXmax(&self, mvccKeyBuffer: &mut BytesMut, dataKey: DataKey) -> Result<KV> {
        mvccKeyBuffer.writeDataMvccXmax(dataKey, self.session.getTxId()?);
        Ok((mvccKeyBuffer.to_vec(), global::EMPTY_BINARY))
    }

    fn generateOrigin(&self, selfDataKey: DataKey, originDataKey: DataKey) -> KV {
        let selfRowId = extract_row_id_from_data_key!(selfDataKey);
        (
            u64_to_byte_array_reference!(key_prefix_add_row_id!(meta::KEY_PPREFIX_ORIGIN_DATA_KEY, selfRowId)).to_vec(),
            u64_to_byte_array_reference!(originDataKey).to_vec()
        )
    }

    /// 当前tx上link的时候 生成的含有xmin 和 xmax 的pointerKey
    fn generateAddPointerXminXmax(&self,
                                  pointerKeyBuffer: &mut BytesMut,
                                  selfDataKey: DataKey,
                                  pointerKeyTag: KeyTag, tableId: TableId, targetDatakey: DataKey) -> Result<(KV, KV)> {
        let xmin = {
            pointerKeyBuffer.writePointerKeyMvccXmin(selfDataKey, pointerKeyTag, tableId, targetDatakey, self.session.getTxId()?);
            (pointerKeyBuffer.to_vec(), global::EMPTY_BINARY) as KV
        };

        let xmax = {
            pointerKeyBuffer.writePointerKeyMvccXmax(selfDataKey, pointerKeyTag, tableId, targetDatakey, meta::TX_ID_INVALID);
            (pointerKeyBuffer.to_vec(), global::EMPTY_BINARY) as KV
        };

        Ok((xmin, xmax))
    }

    /// 当前tx上unlink时候 生成的含有xmax的 pointerKey
    fn generateDeletePointerXmax(&self,
                                 pointerKeyBuffer: &mut BytesMut,
                                 selfDataKey: DataKey,
                                 pointerKeyTag: KeyTag, tableId: TableId, targetDatakey: DataKey) -> Result<KV> {
        pointerKeyBuffer.writePointerKeyMvccXmax(selfDataKey, pointerKeyTag, tableId, targetDatakey, self.session.getTxId()?);
        Ok((pointerKeyBuffer.to_vec(), global::EMPTY_BINARY))
    }
}

trait BytesMutExt {
    // todo writePointerKeyBuffer() 和 writePointerKeyLeadingPart() 有公用部分的 完成
    /// 不包含末尾的对应其它table rel 的dataKey
    fn writePointerKeyLeadingPart(&mut self,
                                  dataKey: DataKey,
                                  keyTag: KeyTag, tableId: TableId);

    // ----------------------------------------------------------------------------

    fn replacePointerKeyMcvvTagTxId(&mut self, pointerKey: &[Byte], mvccKeyTag: KeyTag, txId: TxId);

    fn writePointerKeyMvccXmin(&mut self,
                               selfDatakey: DataKey,
                               pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                               txId: TxId) {
        self.writePointerKey(selfDatakey, pointerKeyTag, targetTableId, targetDataKey, meta::MVCC_KEY_TAG_XMIN, txId)
    }

    fn writePointerKeyMvccXmax(&mut self,
                               selfDatakey: DataKey,
                               pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                               txId: TxId) {
        self.writePointerKey(selfDatakey, pointerKeyTag, targetTableId, targetDataKey, meta::MVCC_KEY_TAG_XMAX, txId)
    }

    fn writePointerKey(&mut self,
                       selfDatakey: DataKey,
                       pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                       pointerMvccKeyTag: KeyTag, txId: TxId);

    // --------------------------------------------------------------------------------

    fn writeDataMvccXmin(&mut self, dataKey: DataKey, xmin: TxId) {
        self.writeDataMvccKey(dataKey, meta::MVCC_KEY_TAG_XMIN, xmin).unwrap();
    }

    fn writeDataMvccXmax(&mut self, dataKey: DataKey, xmax: TxId) {
        self.writeDataMvccKey(dataKey, meta::MVCC_KEY_TAG_XMAX, xmax).unwrap()
    }

    fn writeDataMvccKey(&mut self,
                        dataKey: DataKey,
                        mvccKeyTag: KeyTag,
                        txid: TxId) -> Result<()>;
}

impl BytesMutExt for BytesMut {
    fn writePointerKeyLeadingPart(&mut self,
                                  selfDataKey: DataKey,
                                  keyTag: KeyTag, targetTableId: TableId) {
        self.clear();

        let rowId = extract_row_id_from_data_key!(selfDataKey);
        self.put_u64(key_prefix_add_row_id!(meta::KEY_PREFIX_POINTER, rowId));

        // 写relation的tableId
        self.put_u8(keyTag);
        self.put_u64(targetTableId);

        // 后边用来写dataKey
        self.put_u8(meta::POINTER_KEY_TAG_DATA_KEY);
    }

    fn replacePointerKeyMcvvTagTxId(&mut self, pointerKey: &[Byte], mvccKeyTag: KeyTag, txId: TxId) {
        self.clear();

        self.put_slice(&pointerKey[..meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET]);

        self.put_u8(mvccKeyTag);
        self.put_u64(txId);
    }

    fn writePointerKey(&mut self,
                       selfDatakey: DataKey,
                       pointerKeyTag: KeyTag, tableId: TableId, dataKey: DataKey,
                       pointerMvccKeyTag: KeyTag, txId: TxId) {
        self.writePointerKeyLeadingPart(selfDatakey, pointerKeyTag, tableId);
        self.put_u64(dataKey);
        self.put_u8(pointerMvccKeyTag);
        self.put_u64(txId);
    }

    fn writeDataMvccKey(&mut self,
                        dataKey: DataKey, mvccKeyTag: KeyTag, txid: TxId) -> Result<()> {
        self.clear();

        match mvccKeyTag {
            meta::MVCC_KEY_TAG_XMIN | meta::MVCC_KEY_TAG_XMAX => {
                let rowId = extract_row_id_from_data_key!(dataKey);
                self.put_u64(key_prefix_add_row_id!(meta::KEY_PREFIX_MVCC, rowId));
                self.put_u8(mvccKeyTag);
                self.put_u64(txid);
            }
            _ => throw!("should be KEY_PREFIX_MVCC_XMIN, KEY_PREFIX_MVCC_XMAX"),
        }

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::io::{SeekFrom, Write};
    use serde::{Deserialize, Serialize, Serializer};
    use serde::ser::{SerializeMap, SerializeStruct};
    use serde_json::json;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use crate::graph_value::GraphValue;
    use crate::{byte_slice_to_u64, global, u64_to_byte_array_reference};

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(GraphValue::String("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }

    #[test]
    pub fn testJsonTagged() {
        #[derive(Deserialize)]
        enum A {
            S(String),
        }

        let a = A::S("1".to_string());

        impl Serialize for A {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
                match self {
                    A::S(string) => {
                        if global::UNTAGGED_ENUM_JSON.get() {
                            serializer.serialize_str(string.as_str())
                        } else {
                            // let mut s = serializer.serialize_map(Some(1usize))?;
                            // s.serialize_key("S")?;
                            // s.serialize_value(string)?;

                            let mut s = serializer.serialize_struct("AAAAA", 1)?;
                            s.serialize_field("S", string)?;
                            s.end()
                        }
                    }
                }
            }
        }

        println!("{}", serde_json::to_string(&a).unwrap());

        global::UNTAGGED_ENUM_JSON.set(true);
        println!("{}", serde_json::to_string(&a).unwrap());
    }

    #[tokio::test]
    pub async fn testWriteU64() {
        // 如果设置了append 即使再怎么seek 也只会到末尾append
        let mut file = OpenOptions::new().write(true).read(true).create(true).open("data/user").await.unwrap();
        println!("{}", file.seek(SeekFrom::Start(8)).await.unwrap());
        println!("{}", file.seek(SeekFrom::Current(0)).await.unwrap());

        file.into_std().await.write(&[9]).unwrap();
        //  file.write_u8(9).await.unwrap();
        // file.write_u64(1u64).await.unwrap();
    }

    #[test]
    pub fn testU64Codec() {
        let s = u64_to_byte_array_reference!(2147389121u64);

        let s1 = u64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
        let aa = byte_slice_to_u64!(s);

        println!("{},{}", s1, aa);
    }
}
