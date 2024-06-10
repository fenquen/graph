use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Range, RangeFrom};
use std::sync::atomic::Ordering;
use std::thread;
use bytes::{Bytes, BytesMut};
use rocksdb::{AsColumnFamilyRef, Direction, IteratorMode};
use crate::executor::{CommandExecutor, IterationCmd};
use crate::expr::Expr;
use crate::{byte_slice_to_u64, extractPrefixFromKeySlice, extractTargetDataKeyFromPointerKey, global, keyPrefixAddRowId, meta, suffix_plus_plus, throw, types, u64ToByteArrRef};
use crate::codec::{BinaryCodec, MyBytes};
use crate::graph_value::GraphValue;
use crate::meta::{Column, Table};
use crate::parser::command::insert::Insert;
use crate::parser::element::Element;
use crate::types::{Byte, ColumnFamily, DataKey, DBRawIterator, RowData, TableMutations, ScanCommittedPreProcessor, ScanCommittedPostProcessor, ScanUncommittedPreProcessor, ScanUncommittedPostProcessor, KeyTag, RowId};
use anyhow::Result;
use crate::executor::mvcc::BytesMutExt;
use crate::types::{CommittedPointerKeyProcessor, UncommittedPointerKeyProcessor};

pub(super) struct ScanHooks<A, B, C, D> where A: ScanCommittedPreProcessor,
                                              B: ScanCommittedPostProcessor,
                                              C: ScanUncommittedPreProcessor,
                                              D: ScanUncommittedPostProcessor {
    /// 融合filter读取到committed RowData 前
    pub(super) scanCommittedPreProcessor: Option<A>,
    /// 融合filter读取到committed RowData 后
    pub(super) scanCommittedPostProcessor: Option<B>,
    /// 融合filter读取到uncommitted RowData 前
    pub(super) scanUncommittedPreProcessor: Option<C>,
    /// 融合filter读取到uncommitted RowData 后
    pub(super) scanUncommittedPostProcessor: Option<D>,
}

impl Default for ScanHooks<
    Box<dyn ScanCommittedPreProcessor>,
    Box<dyn ScanCommittedPostProcessor>,
    Box<dyn ScanUncommittedPreProcessor>,
    Box<dyn ScanUncommittedPostProcessor>
> {
    fn default() -> Self {
        ScanHooks {
            scanCommittedPreProcessor: None,
            scanCommittedPostProcessor: None,
            scanUncommittedPreProcessor: None,
            scanUncommittedPostProcessor: None,
        }
    }
}

pub struct SearchPointerKeyHooks<A, B> where A: CommittedPointerKeyProcessor,
                                             B: UncommittedPointerKeyProcessor {
    pub(super) committedPointerKeyProcessor: Option<A>,
    pub(super) uncommittedPointerKeyProcessor: Option<B>,
}

impl Default for SearchPointerKeyHooks<Box<dyn CommittedPointerKeyProcessor>, Box<dyn UncommittedPointerKeyProcessor>> {
    fn default() -> Self {
        SearchPointerKeyHooks {
            committedPointerKeyProcessor: None,
            uncommittedPointerKeyProcessor: None,
        }
    }
}

impl<'session> CommandExecutor<'session> {
    // todo 实现不实际捞取数据的
    /// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
    pub(super) fn getRowDatasByDataKeys(&self,
                                        dataKeys: &[DataKey],
                                        table: &Table,
                                        tableFilter: Option<&Expr>,
                                        selectedColNames: Option<&Vec<String>>) -> Result<Vec<(DataKey, RowData)>> {
        if dataKeys.is_empty() {
            return Ok(Vec::new());
        }

        let mut rowDatas = Vec::with_capacity(dataKeys.len());

        let columnFamily = self.session.getColFamily(&table.name)?;

        let mut mvccKeyBuffer = &mut BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);

        let tableName_mutationsOnTable = self.session.tableName_mutations.borrow();
        let tableMutations: Option<&TableMutations> = tableName_mutationsOnTable.get(&table.name);

        let mut process =
            |dataKey: DataKey| -> Result<()> {
                // todo getRowDatasByDataKeys 增加对uncommitted的区域的搜索 完成
                // 习惯的套路就和scan函数里边1样 都是先搜索committed然后是uncommitted 这对scan来说是可以的
                // 对这边的直接通过datakey获取有点不合适了 搜索uncommitted逻辑要到前边,要是有的话可以提前return
                if let Some(tableMutations) = tableMutations {
                    if self.checkUncommittedDataVisibility(tableMutations, mvccKeyBuffer, dataKey)? {
                        // 是不是不会是none
                        if let Some(addedValueBinary) = tableMutations.get(u64ToByteArrRef!(dataKey).as_ref()) {
                            if let Some(rowData) = self.readRowDataBinary(table, addedValueBinary.as_slice(), tableFilter, selectedColNames)? {
                                rowDatas.push((dataKey, rowData));
                                return Ok(());
                            }
                        }
                    }
                }

                // todo getRowDatasByDataKeys() 也要mvcc筛选 完成
                // mvcc的visibility筛选
                if self.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer,
                                                                       &mut rawIterator,
                                                                       dataKey,
                                                                       &columnFamily,
                                                                       &table.name)? == false {
                    return Ok(());
                }

                if let Some(tableMutations) = tableMutations {
                    if self.checkCommittedDataVisibilityWithTxMutations(tableMutations, &mut mvccKeyBuffer, dataKey)? == false {
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
            // todo 使用rayon 遍历
            for dataKey in dataKeys {
                process(*dataKey)?;
            }
        }

        Ok(rowDatas)
    }

    // todo 实现 index
    // 如果传递的是fn()的话(不是Fn)是函数指针而不是闭包 不能和上下文有联系 闭包返回false 那么 continue
    /// 目前用到hook的地点有 update selectTableUnderRels
    pub(super) fn scanSatisfiedRows<A, B, C, D>(&self, table: &Table,
                                                tableFilter: Option<&Expr>,
                                                selectedColumnNames: Option<&Vec<String>>,
                                                select: bool,
                                                mut scanHooks: ScanHooks<A, B, C, D>) -> Result<Vec<(DataKey, RowData)>>
        where A: ScanCommittedPreProcessor,
              B: ScanCommittedPostProcessor,
              C: ScanUncommittedPreProcessor,
              D: ScanUncommittedPostProcessor {

        // todo 使用table id 为 column family 标识
        let columnFamily = self.session.getColFamily(&table.name)?;

        let tableName_mutationsOnTable = self.session.tableName_mutations.borrow();
        let tableMutationsCurrentTx: Option<&TableMutations> = tableName_mutationsOnTable.get(&table.name);

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);

        let mut satisfiedRows =
            if tableFilter.is_some() || select {
                let mut satisfiedRows = Vec::new();

                let snapshot = self.session.getSnapshot()?;

                // mvcc的visibility筛选
                let mut rawIterator: DBRawIterator = snapshot.raw_iterator_cf(&columnFamily);

                let latestRowId = table.rowIdCounter.load(Ordering::Acquire);
                let scanConcurrency = 2;
                let distance = latestRowId - meta::ROW_ID_INVALID;
                const COUNT_PER_THREAD: u64 = 100000;
                let tail = distance % COUNT_PER_THREAD;
                let mut concurrencyNeed = distance / COUNT_PER_THREAD;
                if concurrencyNeed > 0 {
                    if tail >= COUNT_PER_THREAD / 2 {
                        suffix_plus_plus!(concurrencyNeed);
                    }

                    if concurrencyNeed > scanConcurrency {
                        concurrencyNeed = scanConcurrency;
                    }

                    let interval = distance / concurrencyNeed;

                    let mut ranges: Vec<(DataKey, DataKey)> = Vec::new();

                    let mut lastRoundEnd = meta::ROW_ID_INVALID;

                    for a in 0..concurrencyNeed {
                        let start: RowId = lastRoundEnd + 1 + a * interval;
                        let end: RowId = start + (a + 1) * interval;

                        ranges.push((keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, start), keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, end)));
                    }


                    for (dataKeyStart, dataKeyEnd) in ranges {
                        let tableName = table.name.clone();
                        thread::spawn(move || {
                            let c = self.session.getColFamily(&table.name)?;
                            snapshot.iterator_cf(&c, IteratorMode::From(u64ToByteArrRef!(dataKeyStart), Direction::Forward));

                            Result::<()>::Ok(())
                        });
                    }
                }

                // todo scan遍历能不能concurrent
                // 对data条目而不是pointer条目遍历
                for iterResult in snapshot.iterator_cf(&columnFamily, IteratorMode::From(meta::DATA_KEY_PATTERN, Direction::Forward)) {
                    let (dataKeyBinary, rowDataBinary) = iterResult?;

                    // prefix iterator原理只是seek到prefix对应的key而已 到后边可能会超过范围 https://www.jianshu.com/p/9848a376d41d
                    // 前4个bit的值是不是 KEY_PREFIX_DATA
                    if extractPrefixFromKeySlice!(dataKeyBinary) != meta::KEY_PREFIX_DATA {
                        break;
                    }

                    let dataKey: DataKey = byte_slice_to_u64!(&*dataKeyBinary);

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
                    if let Some(mutationsRawCurrentTx) = tableMutationsCurrentTx {
                        if self.checkCommittedDataVisibilityWithTxMutations(mutationsRawCurrentTx, &mut mvccKeyBuffer, dataKey)? == false {
                            continue;
                        }
                    }

                    if let Some(ref mut scanCommittedPreProcessor) = scanHooks.scanCommittedPreProcessor {
                        if scanCommittedPreProcessor(&columnFamily, dataKey)? == false {
                            continue;
                        }
                    }

                    // mvcc筛选过了 对rowData本身的筛选
                    if let Some(rowData) = self.readRowDataBinary(table, &*rowDataBinary, tableFilter, selectedColumnNames)? {
                        if let Some(ref mut scanCommittedPostProcessor) = scanHooks.scanCommittedPostProcessor {
                            if scanCommittedPostProcessor(&columnFamily, dataKey, &rowData)? == false {
                                continue;
                            }
                        }

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

        // todo scan的时候要搜索uncommitted 完成
        // todo scan的时候也要设置pre和after的钩子函数 完成
        if let Some(tableMutationsCurrentTx) = tableMutationsCurrentTx {
            let addedDataCurrentTxRange =
                tableMutationsCurrentTx.range::<Vec<Byte>, Range<&Vec<Byte>>>(&*meta::DATA_KEY_PATTERN_VEC..&*meta::POINTER_KEY_PATTERN_VEC);

            for (addedDataKeyBinaryCurrentTx, addRowDataBinaryCurrentTx) in addedDataCurrentTxRange {
                let addedDataKeyCurrentTx: DataKey = byte_slice_to_u64!(addedDataKeyBinaryCurrentTx);

                if self.checkUncommittedDataVisibility(tableMutationsCurrentTx, &mut mvccKeyBuffer, addedDataKeyCurrentTx)? == false {
                    continue;
                }

                if let Some(ref mut scanUncommittedPreProcessor) = scanHooks.scanUncommittedPreProcessor {
                    if scanUncommittedPreProcessor(tableMutationsCurrentTx, addedDataKeyCurrentTx)? == false {
                        continue;
                    }
                }

                if let Some(rowData) = self.readRowDataBinary(table, addRowDataBinaryCurrentTx, tableFilter, selectedColumnNames)? {
                    if let Some(ref mut scanUncommittedPostProcessor) = scanHooks.scanUncommittedPostProcessor {
                        if scanUncommittedPostProcessor(tableMutationsCurrentTx, addedDataKeyCurrentTx, &rowData)? == false {
                            continue;
                        }
                    }

                    satisfiedRows.push((addedDataKeyCurrentTx, rowData));
                }
            }
        }

        Ok(satisfiedRows)
    }

    fn readRowDataBinary(&self,
                         table: &Table,
                         rowBinary: &[u8],
                         tableFilter: Option<&Expr>,
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

        if tableFilter.is_none() {
            return Ok(Some(rowData));
        }

        if let GraphValue::Boolean(satisfy) = tableFilter.unwrap().calc(Some(&rowData))? {
            if satisfy {
                Ok(Some(rowData))
            } else {
                Ok(None)
            }
        } else {
            throw!("table filter should get a boolean")
        }
    }

    pub(super) fn generateInsertValuesBinary(&self, insert: &mut Insert, table: &Table) -> anyhow::Result<Bytes> {
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
                let absentColumns: Vec<&Column> =
                    collectionMinus0(&table.columns,
                                     &columnNames,
                                     |column, columnName| { &column.name == columnName });
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

    /// 当前对relation本身的数据的筛选是通过注入闭包实现的
    // todo 如何去应对重复的pointerKey
    // todo pointerKey应该同时到committed和uncommitted去搜索
    pub(super) fn searchPointerKeyByPrefix<A, B>(&self, tableName: &str, prefix: &[Byte],
                                                 mut searchPointerKeyHooks: SearchPointerKeyHooks<A, B>) -> Result<Vec<Box<[Byte]>>>
        where A: CommittedPointerKeyProcessor,
              B: UncommittedPointerKeyProcessor {
        let mut keys = Vec::new();

        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

        let columnFamily = self.session.getColFamily(tableName)?;

        let snapshot = self.session.getSnapshot()?;
        let mut rawIterator = snapshot.raw_iterator_cf(&columnFamily) as DBRawIterator;

        let tableName_mutationsOnTable = self.session.tableName_mutations.borrow();
        let tableMutations: Option<&TableMutations> = tableName_mutationsOnTable.get(tableName);

        // 应对committed
        for iterResult in snapshot.iterator_cf(&columnFamily, IteratorMode::From(prefix, Direction::Forward)) {
            let (committedPointerKey, _) = iterResult?;

            // 说明越过了
            if committedPointerKey.starts_with(prefix) == false {
                break;
            }

            if self.checkCommittedPointerVisiWithoutTxMutations(&mut pointerKeyBuffer,
                                                                &mut rawIterator,
                                                                committedPointerKey.as_ref())? == false {
                continue;
            }

            if let Some(tableMutations) = tableMutations {
                if self.checkCommittedPointerVisiWithTxMutations(tableMutations,
                                                                 &mut pointerKeyBuffer,
                                                                 committedPointerKey.as_ref())? == false {
                    continue;
                }
            }

            if let Some(ref mut committedPointerKeyProcessor) = searchPointerKeyHooks.committedPointerKeyProcessor {
                match committedPointerKeyProcessor(&columnFamily, committedPointerKey.as_ref(), prefix)? {
                    IterationCmd::Break => break,
                    IterationCmd::Continue => continue,
                    IterationCmd::Return => return Ok(keys),
                    IterationCmd::Nothing => {}
                }
            }

            keys.push(committedPointerKey);
        }

        // 应对uncommitted
        if let Some(tableMutations) = tableMutations {
            let addedPointerKeyRange = tableMutations.range::<Vec<Byte>, RangeFrom<&Vec<Byte>>>(&prefix.to_vec()..);

            for (addedPointerKey, _) in addedPointerKeyRange {
                // 因为右边的是未限制的 需要手动
                if addedPointerKey.starts_with(prefix) == false {
                    break;
                }

                if self.checkUncommittedPointerVisi(&tableMutations, &mut pointerKeyBuffer, addedPointerKey)? == false {
                    continue;
                }

                if let Some(ref mut uncommittedPointerKeyProcessor) = searchPointerKeyHooks.uncommittedPointerKeyProcessor {
                    match uncommittedPointerKeyProcessor(tableMutations, addedPointerKey, prefix)? {
                        IterationCmd::Break => break,
                        IterationCmd::Continue => continue,
                        IterationCmd::Return => return Ok(keys),
                        IterationCmd::Nothing => {}
                    }
                }

                keys.push(addedPointerKey.clone().into_boxed_slice());
            }
        }

        Ok(keys)
    }

    /// 以某个pointerKeyPrefix入手(限定打动了targetTableId) 搜索相应的满足条件的
    pub(super) fn searchDataByPointerKeyPrefix(&self,
                                               src: &Table, srcDataKey: DataKey,
                                               pointerKeyTag: KeyTag,
                                               dest: &Table, destFilter: Option<&Expr>) -> Result<Vec<(DataKey, RowData)>> {
        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);
        pointerKeyBuffer.writePointerKeyLeadingPart(srcDataKey, pointerKeyTag, dest.tableId);

        let mut targetRelationDataKeys = Vec::new();

        // 是FnMut 改动了targetRelationDataKeys
        let pointerKeyProcessor = RefCell::new(
            |pointerKey: &[Byte]| {
                let targetRelationDataKey = extractTargetDataKeyFromPointerKey!(pointerKey);
                targetRelationDataKeys.push(targetRelationDataKey);
            }
        );

        let searchPointerKeyHooks = SearchPointerKeyHooks {
            committedPointerKeyProcessor: Some(
                |_: &ColumnFamily, committedPointerKey: &[Byte], _: &[Byte]| {
                    pointerKeyProcessor.borrow_mut()(committedPointerKey);
                    Result::<IterationCmd>::Ok(IterationCmd::Nothing)
                }
            ),
            uncommittedPointerKeyProcessor: Some(
                |_: &TableMutations, addedPointerKey: &[Byte], _: &[Byte]| {
                    pointerKeyProcessor.borrow_mut()(addedPointerKey);
                    Result::<IterationCmd>::Ok(IterationCmd::Nothing)
                }
            ),
        };

        self.searchPointerKeyByPrefix(src.name.as_str(), pointerKeyBuffer.as_ref(), searchPointerKeyHooks)?;

        let relationDatas
            = self.getRowDatasByDataKeys(targetRelationDataKeys.as_slice(), dest, destFilter, None)?;

        Ok(relationDatas)
    }
}





