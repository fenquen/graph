use std::collections::HashMap;
use std::ops::RangeFrom;
use bytes::BytesMut;
use serde_json::{json, Value};
use crate::executor::{CommandExecResult, CommandExecutor, RowData};
use crate::{extract_target_data_key_from_pointer_key, JSON_ENUM_UNTAGGED, meta, suffix_plus_plus, byte_slice_to_u64};
use crate::executor::mvcc::BytesMutExt;
use crate::graph_value::{GraphValue, PointDesc};
use crate::meta::Table;
use crate::parser::{Select, SelectRel, SelectTable};
use crate::types::{Byte, DataKey, KeyTag};
use crate::global;

impl<'session> CommandExecutor<'session> {
    /// 如果不是含有relation的select 便是普通的select
    pub (super) fn select(&self, selectFamily: &Select) -> anyhow::Result<CommandExecResult> {
        match selectFamily {
            // 普通模式不含有relation
            Select::SelectTable(selectTable) => self.selectTable(selectTable),
            Select::SelectRels(selectVec) => self.selectRels(selectVec),
            _ => { panic!("undo") }
        }
    }

    /// 普通的和rdbms相同的 select
    fn selectTable(&self, selectTable: &SelectTable) -> anyhow::Result<CommandExecResult> {
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
}