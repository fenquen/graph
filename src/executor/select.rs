use std::cell::RefCell;
use hashbrown::{HashMap, HashSet};
use std::ops::{Bound, RangeFrom};
use bytes::BytesMut;
use serde_json::{json, Value};
use crate::executor::{CommandExecResult, CommandExecutor, IterationCmd};
use crate::{extractTargetDataKeyFromPointerKey, JSON_ENUM_UNTAGGED,
            meta, suffix_plus_plus, byte_slice_to_u64, types, utils, throw, prefix_minus_minus};
use crate::executor::mvcc::BytesMutExt;
use crate::graph_value::{GraphValue};
use crate::meta::{DBObject, Table};
use crate::types::{Byte, ColumnFamily, DataKey, KeyTag, RowData, DBRawIterator, TableMutations, RelationDepth};
use crate::global;
use crate::parser::command::select::{EndPointType, RelDesc, Select, SelectRel, SelectTable, SelectTableUnderRels};
use anyhow::{anyhow, Result};
use crate::executor::store::{ScanHooks, ScanParams, SearchPointerKeyHooks};
use crate::expr::Expr;
use crate::session::Session;
use crate::types::{CommittedPreProcessor, CommittedPostProcessor, UncommittedPreProcessor, UncommittedPostProcessor};

impl<'session> CommandExecutor<'session> {
    /// 如果不是含有relation的select 便是普通的select
    pub(super) fn select(&self, selectFamily: &Select) -> Result<CommandExecResult> {
        match selectFamily {
            // todo 实现对普通select的 offset limit
            // 普通模式不含有relation
            Select::SelectTable(selectTable) => self.selectTable(selectTable),
            Select::SelectRels(selectVec) => self.selectRels(selectVec),
            Select::SelectTableUnderRels(selectTableUnderRels) => self.selectTableUnderRels(selectTableUnderRels),
        }
    }

    /// 普通的和rdbms相同的 select
    fn selectTable(&self, selectTable: &SelectTable) -> Result<CommandExecResult> {
        let table = Session::getDBObjectByName(selectTable.tableName.as_str())?;

        let table = match table.value() {
            DBObject::Table(t) => t,
            DBObject::Relation(t) => t,
            _ => panic!()
        };
        // let table = table.asTable()?;

        let rowDatas = {
            let scanParams = ScanParams {
                table,
                tableFilter: selectTable.tableFilterExpr.as_ref(),
                selectedColumnNames: selectTable.selectedColNames.as_ref(),
                limit: selectTable.limit,
                offset: selectTable.offset,
                ..Default::default()
            };

            self.scanSatisfiedRows(scanParams, true, ScanHooks::default())?
        };

        let values: Vec<Value> = self.processRowDatasToDisplay(rowDatas);
        // JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&rows)?));

        Ok(CommandExecResult::SelectResult(values))
    }

    /// graph特色的 rel select
    /// ```select user[id,name](id=1 and 0=0) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre```
    fn selectRels(&self, selectRels: &Vec<SelectRel>) -> Result<CommandExecResult> {
        // 对应1个realtion的query的多个条目的1个
        #[derive(Debug)]
        struct SelectResult {
            srcName: String,
            srcRowDatas: Vec<(DataKey, RowData)>,
            /// 目前 当使用recursive后 relation相应当name和data不显示 未想好如何显示
            relationName: Option<String>,
            relationData: Option<RowData>,
            destName: String,
            destRowDatas: Vec<(DataKey, RowData)>,
        }

        // 给next轮用的
        let mut destDataKeysInPrevSelectRel: Option<HashSet<DataKey>> = None;

        // 1个select对应Vec<SelectResult> 多个select对应Vec<Vec<SelectResult>>
        let mut selectResultVecVec: Vec<Vec<SelectResult>> = Vec::with_capacity(selectRels.len());

        'loopSelectRel:
        for selectRel in selectRels {
            // 为什么要使用{} 不然的话有概率死锁
            // https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
            let relationDatas: Vec<(DataKey, RowData)> = {
                let relation = Session::getDBObjectByName(selectRel.relationName.as_str())?;
                let relation = relation.asRelation()?;

                let scanParams = ScanParams {
                    table: relation,
                    tableFilter: selectRel.relationFilter.as_ref(),
                    selectedColumnNames: selectRel.relationColumnNames.as_ref(),
                    ..Default::default()
                };

                // 就像是普通表的搜索,得到满足搜索条件的relation data
                self.scanSatisfiedRows(scanParams, true, ScanHooks::default())?
            };

            let mut selectResultVecInSelectRel = Vec::with_capacity(relationDatas.len());

            // 融合了当前的selectRel的满足条件的全部的relationDatas的全部的destDataKey
            let mut destDataKeysInSelectRel = HashSet::new();

            let srcTable = Session::getDBObjectByName(&selectRel.srcTableName)?;
            let srcTable = srcTable.asTable()?;

            let destTable = Session::getDBObjectByName(selectRel.destTableName.as_str())?;
            let destTable = destTable.asTable()?;

            let relation = Session::getDBObjectByName(selectRel.relationName.as_str())?;
            let relation = relation.asRelation()?;

            // 遍历当前的selectRel的多条relationData
            'loopRelationData:
            for (relationDataKey, relationData) in relationDatas {
                let gatherTargetDatas =
                    |pointerKeyTag: KeyTag, targetTable: &Table, targetFilter: Option<&Expr>| {
                        // todo selectRels时候如何应对pointerKey的mvcc 完成
                        let targetDatas = self.searchDataByPointerKeyPrefix(relation, relationDataKey, pointerKeyTag, targetTable, targetFilter)?;

                        // todo 不知道要不要dedup
                        Result::<Vec<(DataKey, RowData)>>::Ok(targetDatas)
                    };

                // 收罗该rel上的全部的src的dataKey
                let mut srcRowDatas = {
                    let srcRowDatas = gatherTargetDatas(meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable, selectRel.srcFilter.as_ref())?;

                    if srcRowDatas.is_empty() {
                        continue 'loopRelationData;
                    }

                    srcRowDatas
                };

                // 收罗该rel上的全部的dest的dataKey
                let mut destRowDatas =
                    if selectRel.relationDepth.is_none() {
                        let destRowDatas =
                            gatherTargetDatas(meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable, selectRel.destFilter.as_ref())?;

                        if destRowDatas.is_empty() {
                            continue 'loopRelationData;
                        }

                        destRowDatas
                    } else {
                        Vec::new()
                    };

                let srcRowDatas = {
                    let srcRowDatas =
                        match destDataKeysInPrevSelectRel {
                            Some(ref destDataKeysInPrevSelect) => {
                                let srcDataKeys: Vec<DataKey> = srcRowDatas.iter().map(|(srcDataKey, _)| *srcDataKey).collect();

                                // 上轮的全部的各个条目里边的destDataKeys 和 当前条目的srcDataKeys的交集
                                let intersectDataKeys: Vec<DataKey> =
                                    destDataKeysInPrevSelect.iter().filter(|&&destDataKeyPrevSelect| srcDataKeys.contains(&destDataKeyPrevSelect)).map(|destDataKeyInPrevSelect| *destDataKeyInPrevSelect).collect();

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
                                        break 'loopSelectRel;
                                    }
                                }

                                srcRowDatas.retain(|(srcDataKey, _)| intersectDataKeys.contains(srcDataKey));

                                srcRowDatas
                            }
                            // 第1 round
                            None => srcRowDatas,
                        };

                    if srcRowDatas.is_empty() {
                        continue 'loopRelationData;
                    }

                    srcRowDatas
                };


                // 收集了当前的relationData的destDataKeys到
                if selectRel.relationDepth.is_none() {
                    for (destDataKey, _) in &destRowDatas {
                        destDataKeysInSelectRel.insert(*destDataKey);
                    }
                }

                // todo selectRelRecursive 如何应对环状的情况 尤其是在不限制endDepth时候
                // parse的时候已经限制了 要是recursive 是 [1..2) 之类的本质和没有recusive相同 会直接拦掉
                if let Some(relationDepth) = selectRel.relationDepth {
                    let mut a = HashMap::new();

                    match relationDepth {
                        (Bound::Included(startDepth), Bound::Included(endDepth)) => {
                            let mut initial: Vec<DataKey> = srcRowDatas.iter().map(|(srcDataKey, _)| *srcDataKey).collect();

                            for depth in startDepth..=endDepth {
                                // 例如recursive[9,13] 要下钻 9,10,11,12,13 这几个深度的
                                // 那么第1趟下钻9级会比较辛苦,后边的10级 只要依赖9级下钻的成果便可以了
                                let depth =
                                    if depth == startDepth {  // 第1趟的时候
                                        depth
                                    } else {
                                        1
                                    };

                                let destRowDatasRecursive =
                                    self.selectRelRecursive(initial, srcTable, selectRel.srcFilter.as_ref(),
                                                            meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID,
                                                            relation, selectRel.relationFilter.as_ref(),
                                                            meta::POINTER_KEY_TAG_DEST_TABLE_ID,
                                                            depth)?;

                                // 不用再钻了
                                if destRowDatasRecursive.is_empty() {
                                    break;
                                }

                                initial = destRowDatasRecursive.iter().map(|(destDataKeyRecursive, _)| *destDataKeyRecursive).collect();

                                for destDataKeyRecursive in destRowDatasRecursive {
                                    destDataKeysInSelectRel.insert(destDataKeyRecursive.0);
                                    a.insert(destDataKeyRecursive.0, destDataKeyRecursive.1);
                                }
                            }
                        }
                        _ => panic!("impossible"),
                    };

                    destRowDatas = a.into_iter().map(|entry| { entry }).collect();
                }

                if destRowDatas.is_empty() {
                    continue 'loopRelationData;
                }

                // 当前使用递归的话不显示relation 因为尚未的想好如何显示
                let selectResult =
                    if selectRel.relationDepth.is_some() {
                        SelectResult {
                            srcName: selectRel.srcAlias.as_ref().unwrap_or_else(|| &selectRel.srcTableName).clone(),
                            srcRowDatas,
                            relationName: None,
                            relationData: None,
                            destName: selectRel.destAlias.as_ref().unwrap_or_else(|| &selectRel.destTableName).clone(),
                            destRowDatas,
                        }
                    } else {
                        SelectResult {
                            srcName: selectRel.srcAlias.as_ref().unwrap_or_else(|| &selectRel.srcTableName).clone(),
                            srcRowDatas,
                            relationName: Some(selectRel.relationAlias.as_ref().unwrap_or_else(|| &selectRel.relationName).clone()),
                            relationData: Some(relationData),
                            destName: selectRel.destAlias.as_ref().unwrap_or_else(|| &selectRel.destTableName).clone(),
                            destRowDatas,
                        }
                    };

                selectResultVecInSelectRel.push(selectResult);
            }

            // 到了这边遍历relationData结束

            destDataKeysInPrevSelectRel = {
                // 当前的relation select 的多个realtion对应destDataKey全都是empty的
                if destDataKeysInSelectRel.is_empty() {
                    // todo 是不是应该全都没有了
                    break 'loopSelectRel;
                }

                Some(destDataKeysInSelectRel)
            };

            selectResultVecVec.push(selectResultVecInSelectRel);
        }

        /// ```[[[第1个select的第1行data],[第1个select的第2行data]],[[第2个select的第1行data],[第2个select的第2行data]]]```
        /// 到时候要生成4条脉络
        fn handleResult(selectResultVecVec: Vec<Vec<SelectResult>>) -> Vec<Value> {
            let mut valueVec = Vec::new();

            if selectResultVecVec.is_empty() {
                return valueVec;
            }

            // level0上横向遍历
            for selectResult in &selectResultVecVec[0] {
                let mut json = json!({});

                // 把tuple的position干掉
                let srcRowDatas: Vec<&RowData> = selectResult.srcRowDatas.iter().map(|(_, rownData)| rownData).collect();
                let destRowDatas: Vec<&RowData> = selectResult.destRowDatas.iter().map(|(_, rowData)| rowData).collect();

                // 对json::Value来说需要注意的是serialize的调用发生在这边 而不是serde_json::to_string()
                json[selectResult.srcName.as_str()] = json!(srcRowDatas);
                if selectResult.relationName.is_some() {
                    json[selectResult.relationName.as_ref().unwrap().as_str()] = json!(selectResult.relationData.as_ref().unwrap());
                }
                json[selectResult.destName.as_str()] = json!(destRowDatas);

                let mut selectVecResultVecVecIndex = 1usize;
                loop {
                    // 深度上向下
                    let outerIndex = suffix_plus_plus!(selectVecResultVecVecIndex);
                    if outerIndex == selectResultVecVec.len() {
                        break;
                    }

                    for selectResult in selectResultVecVec.get(outerIndex).unwrap() {
                        if selectResult.relationName.is_some() {
                            json[selectResult.relationName.as_ref().unwrap().as_str()] = json!(selectResult.relationData.as_ref().unwrap());
                        }

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

    /// select user(id = 1 ) as user0 ,in usage (number = 7) ,end in own(number =7) <br>
    /// 逻辑如下 <br>
    /// scan(committed uncommitted)满足要求的node数据 <br>
    /// searchPointerKeys得到(committed uncommitted)属于node的,满足位置要求的,指向relation的pointerKey <br>
    /// 到pointerKey提取relation的dataKey <br>
    /// 调用getByDataKeys(committed uncommitted) 融合 对relarion的过滤条件 确定 relation是不是满足
    /// <br>
    /// 相当是在原来基础上再加上对data指向的rel的筛选
    fn selectTableUnderRels(&self, selectTableUnderRels: &SelectTableUnderRels) -> Result<CommandExecResult> {
        // 先要以普通select table体系筛选 然后对pointerKey筛选
        let table = Session::getDBObjectByName(selectTableUnderRels.selectTable.tableName.as_str())?;
        let table = table.asTable()?;

        let mut pointerKeyBuffer = self.withCapacityIn(meta::POINTER_KEY_BYTE_LEN);

        // 应对对当前的data条目的 对某个relDesc的相应要求
        let mut processRelDesc =
            |nodeDataKey: DataKey,
             pointerKeyTag: KeyTag,
             relDesc: &RelDesc,
             relation: &Table| {
                let mut found = false;

                // 钩子
                // checkTargetRelSatisfy 改变环境变量found 是FnMut 而且下边的searchPointerKeyHooks的会重复使用
                // 不使用RefCell的话会报错 可变引用不能同时有多个
                let checkTargetRelSatisfy = RefCell::new(
                    |pointerKey: &[Byte]| { // node的
                        // 对rel的data本身筛选
                        // todo 只是提示 已提交的pointerKey指向的对象必然只是在已提交的区域
                        // 得到relation数据的dataKey
                        let targetRelationDataKey = extractTargetDataKeyFromPointerKey!(pointerKey);

                        let mut scanParams = ScanParams::default();
                        scanParams.table = relation;
                        scanParams.tableFilter = relDesc.relationFliter.as_ref();
                        scanParams.selectedColumnNames = None;

                        // relation数据是不是满足relationFliter
                        if self.getRowDatasByDataKeys(&[targetRelationDataKey], &scanParams, &mut ScanHooks::default())?.len() > 0 {
                            found = true;
                            return Result::<IterationCmd>::Ok(IterationCmd::Return);
                        }

                        // 不存在对应的relationData 需要continue继续搜索
                        Result::<IterationCmd>::Ok(IterationCmd::Continue)
                    }
                );

                let searchPointerKeyHooks = SearchPointerKeyHooks {
                    committedPointerKeyProcessor: Some(
                        |_: &ColumnFamily, committedPointerKey: &[Byte], _: &[Byte]| {
                            checkTargetRelSatisfy.borrow_mut()(committedPointerKey)
                        }
                    ),
                    uncommittedPointerKeyProcessor: Some(
                        |_: &TableMutations, addedPointerKey: &[Byte], _: &[Byte]| {
                            checkTargetRelSatisfy.borrow_mut()(addedPointerKey)
                        }
                    ),
                };

                // 如果是起点的话 那么rel便是它的downstream
                // 搜寻满足和当前table data的相互地位的rel的data 遍历的是rel
                pointerKeyBuffer.writePointerKeyLeadingPart(nodeDataKey, pointerKeyTag, relation.id);

                // 本node指向rel的pointerKey的前缀
                let pointerKeyPrefix = pointerKeyBuffer.to_vec();

                // 遍历这个node上的通过了测试的指向rel的pointerKey
                // 通过调用getRowDatasByDataKeys得到pointerKey指向的relation数据
                // 融合relationFliter确定relation数据是不是满足要求
                self.searchPointerKeyByPrefix(table.id, &pointerKeyPrefix, searchPointerKeyHooks)?;

                Result::<bool>::Ok(found)
            };

        // 闭包同时被两个共用 如不使用RefCell会报错闭包不能被同时多趟&mut
        let processNodeDataKey = RefCell::new(
            |nodeDataKey: DataKey| {
                // 遍历relDesc,看data是不是都能满足
                for relDesc in &selectTableUnderRels.relDescVec {
                    let relation = Session::getDBObjectByName(relDesc.relationName.as_str())?;
                    let relation = relation.asRelation()?;

                    // 是不是能满足当前relDesc要求
                    let satisfyRelDesc =
                        match relDesc.endPointType {
                            // 闭包里边镶嵌闭包
                            EndPointType::Start => processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relDesc, relation)?,
                            EndPointType::End => processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relDesc, relation)?,
                            EndPointType::Either => {
                                processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relDesc, relation)? ||
                                    processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relDesc, relation)?
                            }
                        };

                    if satisfyRelDesc == false {
                        return Result::<bool>::Ok(false);
                    }
                }

                Result::<bool>::Ok(true)
            }
        );

        let scanHooks = ScanHooks {
            // 确认当前的data是不是满足在各个rel上的位置
            committedPreProcessor: Some(
                |_: &ColumnFamily, committedDataKey: DataKey| {
                    processNodeDataKey.borrow_mut()(committedDataKey)
                }
            ),
            committedPostProcessor: Option::<Box<dyn CommittedPostProcessor>>::None,
            // todo uncommitted也要照顾到 完成
            // 到mutations上去搜索相应的pointerKey的
            uncommittedPreProcessor: Some(
                |_: &TableMutations, addedDatakey: DataKey| {
                    processNodeDataKey.borrow_mut()(addedDatakey)
                }
            ),
            uncommittedPostProcessor: Option::<Box<dyn UncommittedPostProcessor>>::None,
        };

        let rowDatas = {
            let scanParams = ScanParams {
                table,
                tableFilter: selectTableUnderRels.selectTable.tableFilterExpr.as_ref(),
                selectedColumnNames: selectTableUnderRels.selectTable.selectedColNames.as_ref(),
                ..Default::default()
            };

            self.scanSatisfiedRows(scanParams, true, scanHooks)?
        };

        let values = self.processRowDatasToDisplay(rowDatas);

        Ok(CommandExecResult::SelectResult(values))
    }

    /// 使用 由端点不断下钻的套路
    pub(super) fn selectRelRecursive(&self,
                                     srcDataKeys: Vec<DataKey>, table: &Table, filter: Option<&Expr>,
                                     pointerKeyTagOnNode: KeyTag,
                                     relation: &Table, relationFilter: Option<&Expr>,
                                     pointerKeyTagOnRelation: KeyTag,
                                     mut depthRemaining: usize) -> Result<Vec<(DataKey, RowData)>> {
        let lastRound = depthRemaining == 1;

        // 和 srcDataKeys 对应的 destDataKeys,是融合1起的
        let mut destRowDataKeysTotal = Vec::new();
        let mut destRowDataTotal = Vec::new();

        // 多个srcDataKey -> 多个relationData -> 多个destDatas
        for srcDataKey in srcDataKeys {
            // 得到对应的relation
            // relationFilter在过程中是都要的
            let relationDatas =
                self.searchDataByPointerKeyPrefix(table, srcDataKey, pointerKeyTagOnNode, relation, relationFilter)?;

            // 遍历各个relationData
            for (relationDataKey, _) in &relationDatas {
                // 得到这个relationDataKey上的destDataKeys
                let destRowDatas =
                    // 过程中对dest没有过滤需要
                    if lastRound == false {
                        self.searchDataByPointerKeyPrefix(relation, *relationDataKey, pointerKeyTagOnRelation, table, None)?
                    } else {  // 说明已到了最后了,需要对destDataKeys使用filter
                        self.searchDataByPointerKeyPrefix(relation, *relationDataKey, pointerKeyTagOnRelation, table, filter)?
                    };

                for (destDataKey, destRowData) in destRowDatas {
                    destRowDataKeysTotal.push(destDataKey);

                    if lastRound {
                        destRowDataTotal.push((destDataKey, destRowData));
                    }
                }
            }
        }

        if destRowDataKeysTotal.is_empty() {
            return Ok(Vec::new());
        }

        if lastRound {
            return Ok(destRowDataTotal);
        }

        // 不断递归,以destRowDataKeysTotal起点再向下
        self.selectRelRecursive(destRowDataKeysTotal, table, filter,
                                pointerKeyTagOnNode,
                                relation, relationFilter,
                                pointerKeyTagOnRelation,
                                prefix_minus_minus!(depthRemaining))
    }

    #[inline]
    fn processRowDatasToDisplay(&self, rowDatas: Vec<(DataKey, RowData)>) -> Vec<Value> {
        JSON_ENUM_UNTAGGED!(rowDatas.into_iter().map(|(dataKey,rowData)| serde_json::to_value(&rowData).unwrap()).collect())
    }
}