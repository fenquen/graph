use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::RangeFrom;
use bytes::BytesMut;
use serde_json::{json, Value};
use crate::executor::{CommandExecResult, CommandExecutor, IterationCmd};
use crate::{extractTargetDataKeyFromPointerKey, JSON_ENUM_UNTAGGED, meta, suffix_plus_plus, byte_slice_to_u64, types};
use crate::executor::mvcc::BytesMutExt;
use crate::graph_value::{GraphValue, PointDesc};
use crate::meta::Table;
use crate::types::{Byte, ColumnFamily, DataKey, KeyTag, RowData, DBRawIterator, TableMutations};
use crate::global;
use crate::parser::command::select::{EndPointType, RelDesc, Select, SelectRel, SelectTable, SelectTableUnderRels};
use anyhow::{anyhow, Result};
use crate::executor::store::{ScanHooks, SearchPointerKeyHooks};
use crate::types::{ScanCommittedPreProcessor, ScanCommittedPostProcessor, ScanUncommittedPreProcessor, ScanUncommittedPostProcessor};

impl<'session> CommandExecutor<'session> {
    /// 如果不是含有relation的select 便是普通的select
    pub(super) fn select(&self, selectFamily: &Select) -> Result<CommandExecResult> {
        match selectFamily {
            // 普通模式不含有relation
            Select::SelectTable(selectTable) => self.selectTable(selectTable),
            Select::SelectRels(selectVec) => self.selectRels(selectVec),
            Select::SelectTableUnderRels(selectTableUnderRels) => self.selectTableUnderRels(selectTableUnderRels),
        }
    }

    /// 普通的和rdbms相同的 select
    fn selectTable(&self, selectTable: &SelectTable) -> Result<CommandExecResult> {
        let table = self.getTableRefByName(selectTable.tableName.as_str())?;

        let rowDatas = {
            self.scanSatisfiedRows(table.value(),
                                   selectTable.tableFilterExpr.as_ref(),
                                   selectTable.selectedColNames.as_ref(),
                                   true,
                                   ScanHooks {
                                       scanCommittedPreProcessor: Option::<Box<dyn ScanCommittedPreProcessor>>::None,
                                       scanCommittedPostProcessor: Option::<Box<dyn crate::types::ScanCommittedPostProcessor>>::None,
                                       scanUncommittedPreProcessor: Option::<Box<dyn crate::types::ScanUncommittedPreProcessor>>::None,
                                       scanUncommittedPostProcessor: Option::<Box<dyn crate::types::ScanUncommittedPostProcessor>>::None,
                                   })?
        };

        let values: Vec<Value> = self.processRowDatasToDisplay(rowDatas);
        // JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&rows)?));

        Ok(CommandExecResult::SelectResult(values))
    }

    /// graph特色的 rel select
    fn selectRels(&self, selectVec: &Vec<SelectRel>) -> anyhow::Result<CommandExecResult> {
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
            // 为什么要使用{} 不然的话有概率死锁
            // https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
            let relationDatas: Vec<(DataKey, RowData)> = {
                let relation = self.getTableRefByName(select.relationName.as_ref().unwrap())?;
                let relationDatas =
                    self.scanSatisfiedRows(relation.value(),
                                           select.relationFliterExpr.as_ref(),
                                           select.relationColumnNames.as_ref(),
                                           true, ScanHooks::default())?;
                relationDatas.into_iter().map(|(dataKey, rowData)| (dataKey, rowData)).collect()
            };

            let mut selectResultVecInCurrentSelect = Vec::with_capacity(relationDatas.len());

            // 融合了当前的select的relationDatas的全部的dest的dataKey
            let mut destKeysInCurrentSelect = vec![];

            let srcTable = self.getTableRefByName(&select.srcTableName)?;
            let destTable = self.getTableRefByName(select.destTableName.as_ref().unwrap())?;

            let relColFamily = self.session.getColFamily(select.relationName.as_ref().unwrap())?;

            // 遍历当前的select的多个relation
            'loopRelationData:
            for (relationDataKey, relationData) in relationDatas {
                let mut gatherTargetDataKeys =
                    |keyTag: KeyTag, targetTable: &Table| {
                        pointerKeyPrefixBuffer.writePointerKeyLeadingPart(relationDataKey, keyTag, targetTable.tableId);

                        // todo selectRels时候如何应对pointerKey的mvcc 完成
                        let mut pointerKeys =
                            self.searchPointerKeyByPrefix(select.relationName.as_ref().unwrap(),
                                                          &relColFamily,
                                                          pointerKeyPrefixBuffer.as_ref(),
                                                          SearchPointerKeyHooks::default())?;

                        let targetDataKeys = pointerKeys.into_iter().map(|pointerKey| extractTargetDataKeyFromPointerKey!(&*pointerKey)).collect::<Vec<DataKey>>();

                        // todo 不知道要不要dedup
                        Result::<Vec<DataKey>>::Ok(targetDataKeys)
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

    /// select user(id = 1 ) as user0 ,in usage (number = 7) ,end in own(number =7) <br>
    /// 逻辑如下 <br>
    /// scan(committed uncommitted)满足要求的node数据 <br>
    /// 搜索(committed uncommitted)属于node的,满足位置要求的,指向relation的pointerKey <br>
    /// 到pointerKey提取relation的dataKey <br>
    /// 调用getByDataKeys(committed uncommitted) 融合 对relarion的过滤条件 确定 relation是不是满足
    fn selectTableUnderRels(&self, selectTableUnderRels: &SelectTableUnderRels) -> Result<CommandExecResult> {
        // 先要以普通select table体系筛选 然后对pointerKey筛选
        let table = self.getTableRefByName(selectTableUnderRels.selectTable.tableName.as_str())?;
        let columnFamily = self.session.getColFamily(table.name.as_str())?;

        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

        // 应对对当前的data条目的 对某个relDesc的相应要求
        let mut processRelDesc =
            |nodeDataKey: DataKey, pointerKeyTag: KeyTag, relDesc: &RelDesc, relation: &Table| {
                // 如果是起点的话 那么rel便是它的downstream
                // 搜寻满足和当前table data的相互地位的rel的data 遍历的是rel
                pointerKeyBuffer.writePointerKeyLeadingPart(nodeDataKey, pointerKeyTag, relation.tableId);

                // 本node指向rel的pointerKey的前缀
                let pointerKeyPrefix = pointerKeyBuffer.to_vec();

                let mut found = false;

                // 钩子
                let pointerKeyProcessor = RefCell::new(
                    |pointerKey: &[Byte]| {
                        // 对rel的data本身筛选
                        // todo 只是提示 已提交的pointerKey指向的对象必然只是在已提交的区域
                        // 得到relation数据的dataKey
                        let targetRelationDataKey = extractTargetDataKeyFromPointerKey!(pointerKey);

                        // relation数据是不是满足relationFliter
                        if self.getRowDatasByDataKeys(&[targetRelationDataKey],
                                                      relation,
                                                      relDesc.relationFliter.as_ref(),
                                                      None)?.len() > 0 {
                            found = true;
                            return Result::<IterationCmd>::Ok(IterationCmd::Return);
                        }

                        Result::<IterationCmd>::Ok(IterationCmd::Continue)
                    }
                );

                let serchPointerKeyHooks = SearchPointerKeyHooks {
                    committedPointerKeyProcessor: Some(
                        |columnFamily: &ColumnFamily, committedPointerKey: &[Byte]| {
                            pointerKeyProcessor.borrow_mut()(committedPointerKey)
                        }
                    ),
                    uncommittedPointerKeyProcessor: Some(
                        |tableMutations: &TableMutations, addedPointerKey: &[Byte]| {
                            pointerKeyProcessor.borrow_mut()(addedPointerKey)
                        }
                    ),
                };

                // 遍历这个node上的通过了测试的指向rel的pointerKey
                // 通过调用getRowDatasByDataKeys得到pointerKey指向的relation数据
                // 融合relationFliter确定relation数据是不是满足要求
                self.searchPointerKeyByPrefix(table.name.as_str(), &columnFamily, &pointerKeyPrefix, serchPointerKeyHooks)?;

                Result::<bool>::Ok(found)
            };

        // 闭包同时被两个共用 如不使用RefCell会报错闭包不能被同时多趟&mut
        let processNodeDataKey = RefCell::new(
            |nodeDataKey: DataKey| {
                // 遍历relDesc,看data是不是都能满足
                for relDesc in &selectTableUnderRels.relDescVec {
                    let relation = self.getTableRefByName(relDesc.relationName.as_str())?;

                    // 是不是能满足当前relDesc要求
                    let satisfyRelDesc =
                        match relDesc.endPointType {
                            // 闭包里边镶嵌闭包
                            EndPointType::Start => processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relDesc, relation.value())?,
                            EndPointType::End => processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relDesc, relation.value())?,
                            EndPointType::Either => {
                                processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relDesc, relation.value())? ||
                                    processRelDesc(nodeDataKey, meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relDesc, relation.value())?
                            }
                        };

                    if satisfyRelDesc == false {
                        return Result::<bool>::Ok(false);
                    }
                }

                Result::<bool>::Ok(true)
            }
        );

        // 确认当前的data是不是满足在各个rel上的位置
        let scanCommittedPreProcessor =
            |columnFamily: &ColumnFamily, committedDataKey: DataKey| {
                processNodeDataKey.borrow_mut()(committedDataKey)
            };

        // todo uncommitted也要照顾到
        // 到mutations上去搜索相应的pointerKey的
        let scanUncommittedPreProcessor =
            |tableMutations: &TableMutations, addedDatakey: DataKey| {
                processNodeDataKey.borrow_mut()(addedDatakey)
            };

        let scanHooks = ScanHooks {
            scanCommittedPreProcessor: Some(scanCommittedPreProcessor),
            scanCommittedPostProcessor: Option::<Box<dyn ScanCommittedPostProcessor>>::None,
            scanUncommittedPreProcessor: Some(scanUncommittedPreProcessor),
            scanUncommittedPostProcessor: Option::<Box<dyn ScanUncommittedPostProcessor>>::None,
        };

        let rowDatas =
            self.scanSatisfiedRows(table.value(),
                                   selectTableUnderRels.selectTable.tableFilterExpr.as_ref(),
                                   selectTableUnderRels.selectTable.selectedColNames.as_ref(),
                                   true, scanHooks)?;

        let values = self.processRowDatasToDisplay(rowDatas);

        Ok(CommandExecResult::SelectResult(values))
    }

    fn processRowDatasToDisplay(&self, rowDatas: Vec<(DataKey, RowData)>) -> Vec<Value> {
        let rowDatas: Vec<RowData> = rowDatas.into_iter().map(|(_, rowData)| rowData).collect();
        JSON_ENUM_UNTAGGED!(rowDatas.into_iter().map(|rowData| serde_json::to_value(&rowData).unwrap()).collect())
    }
}