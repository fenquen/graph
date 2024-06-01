use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use bytes::{Bytes, BytesMut};
use rocksdb::{AsColumnFamilyRef, Direction, IteratorMode};
use crate::executor::{CommandExecutor, RowData};
use crate::expr::Expr;
use crate::{byte_slice_to_u64, extract_prefix_from_key_slice, global, meta, throw, u64ToByteArrRef};
use crate::codec::{BinaryCodec, MyBytes};
use crate::graph_value::GraphValue;
use crate::meta::{Column, Table};
use crate::parser::{Element, Insert};
use crate::types::{Byte, ColumnFamily, DataKey, DBRawIterator};

impl<'session> CommandExecutor<'session> {
    /// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
    pub fn getRowDatasByDataKeys(&self,
                                 dataKeys: &[DataKey],
                                 table: &Table,
                                 tableFilter: Option<&Expr>,
                                 selectedColNames: Option<&Vec<String>>) -> anyhow::Result<Vec<(DataKey, RowData)>> {
        let mut rowDatas = Vec::with_capacity(dataKeys.len());

        let columnFamily = self.session.getColFamily(&table.name)?;

        let mut mvccKeyBuffer = &mut BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);

        let tableName_mutationsOnTable = self.session.tableName_mutationsOnTable.borrow();
        let mutationsRawCurrentTx = tableName_mutationsOnTable.get(&table.name);

        let mut process =
            |dataKey: DataKey| -> anyhow::Result<()> {
                // todo getRowDatasByDataKeys() 也要mvcc筛选 完成
                // mvcc的visibility筛选
                if self.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer,
                                                                       &mut rawIterator,
                                                                       dataKey,
                                                                       &columnFamily,
                                                                       &table.name)? == false {
                    return Ok(());
                }

                if let Some(mutationsRawCurrentTx) = mutationsRawCurrentTx {
                    if self.checkCommittedDataVisibilityWithTxMutations(mutationsRawCurrentTx, &mut mvccKeyBuffer, dataKey)? == false {
                        return Ok(());
                    }
                }

                let rowDataBinary =
                    match self.session.getSnapshot()?.get_cf(&columnFamily, u64ToByteArrRef!(dataKey))? {
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
    pub fn scanSatisfiedRows(&self, table: &Table,
                             tableFilter: Option<&Expr>,
                             selectedColumnNames: Option<&Vec<String>>,
                             select: bool,
                             rowChecker: Option<fn(commandExecutor: &CommandExecutor,
                                                   columnFamily: &ColumnFamily,
                                                   dataKey: DataKey) -> anyhow::Result<bool>>) -> anyhow::Result<Vec<(DataKey, RowData)>> {
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
                        if rowChecker(self, &columnFamily, dataKey)? == false {
                            continue;
                        }
                    }

                    // mvcc的visibility筛选
                    if self.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer,
                                                                           &mut rawIterator,
                                                                           dataKey,
                                                                           &columnFamily,
                                                                           &table.name)? == false {
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
                    rawIterator.seek_for_prev(u64ToByteArrRef!(((meta::KEY_PREFIX_DATA + 1) as u64)  << meta::ROW_ID_BIT_LEN));
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

    fn readRowDataBinary(&self,
                         table: &Table,
                         rowBinary: &[u8],
                         tableFilterExpr: Option<&Expr>,
                         selectedColumnNames: Option<&Vec<String>>) -> anyhow::Result<Option<RowData>> {
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

    pub fn generateInsertValuesBinary(&self, insert: &mut Insert, table: &Table) -> anyhow::Result<Bytes> {
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

    pub fn getKeysByPrefix(&self,
                           tableName: &str,
                           colFamily: &impl AsColumnFamilyRef,
                           prefix: &[Byte],
                           // 和上下文有联系的闭包不能使用fn来表示 要使用Fn的tarit表示 fn是函数指针只和入参有联系 它可以用Fn的trait表达
                           filterWithoutMutation: Option<fn(&CommandExecutor<'session>,
                                                            pointerKeyBuffer: &mut BytesMut,
                                                            rawIterator: &mut DBRawIterator,
                                                            pointerKey: &[Byte]) -> anyhow::Result<bool>>,
                           filterWithMutation: Option<fn(&CommandExecutor<'session>,
                                                         mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                         pointerKeyBuffer: &mut BytesMut,
                                                         pointerKey: &[Byte]) -> anyhow::Result<bool>>) -> anyhow::Result<Vec<Box<[Byte]>>> {
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
}