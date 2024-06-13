use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Range, RangeFrom};
use std::sync::atomic::Ordering;
use std::{mem, thread};
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use bytes::{Bytes, BytesMut};
use rocksdb::{AsColumnFamilyRef, Direction, IteratorMode};
use crate::executor::{CommandExecutor, IterationCmd};
use crate::expr::Expr;
use crate::{byte_slice_to_u64, extractPrefixFromKeySlice, extractTargetDataKeyFromPointerKey, keyPrefixAddRowId, suffix_plus_plus, throw, u64ToByteArrRef, global, meta, types, prefix_plus_plus};
use crate::codec::{BinaryCodec, MyBytes};
use crate::graph_value::GraphValue;
use crate::meta::{Column, Table};
use crate::parser::command::insert::Insert;
use crate::parser::element::Element;
use crate::types::{Byte, ColumnFamily, DataKey, DBRawIterator, RowData, TableMutations, KeyTag, RowId};
use crate::types::{ScanCommittedPreProcessor, ScanCommittedPostProcessor, ScanUncommittedPreProcessor, ScanUncommittedPostProcessor};
use anyhow::Result;
use lazy_static::lazy_static;
use crate::executor::mvcc::BytesMutExt;
use crate::session::Session;
use crate::types::{CommittedPointerKeyProcessor, UncommittedPointerKeyProcessor};

pub(super) struct ScanHooks<A, B, C, D>
    where
        A: ScanCommittedPreProcessor,
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

pub struct SearchPointerKeyHooks<A, B>
    where
        A: CommittedPointerKeyProcessor,
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

/// 随着scan函数的参数越来越多,是有必要将它们都收拢到1道
pub(super) struct ScanParams<'Table, 'TableFilter, 'SelectedColumnNames> {
    pub table: &'Table Table,
    pub tableFilter: Option<&'TableFilter Expr>,
    pub selectedColumnNames: Option<&'SelectedColumnNames Vec<String>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

lazy_static! {
    static ref T: Table = Table::default();
}

// 如何对含有引用的struct生成default
// https://stackoverflow.com/questions/66609014/how-can-i-implement-default-for-struct
impl<'Table> Default for ScanParams<'Table, '_, '_> {
    fn default() -> Self {
        ScanParams {
            table: &T,
            tableFilter: None,
            selectedColumnNames: None,
            limit: None,
            offset: None,
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

        let columnFamily = Session::getColFamily(&table.name)?;

        let mut mvccKeyBuffer = &mut BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);

        let tableName_mutationsOnTable = self.session.tableName_mutations.read().unwrap();
        let tableMutations: Option<&TableMutations> = tableName_mutationsOnTable.get(&table.name);

        let mut processDataKey =
            |dataKey: DataKey, sender: Option<SyncSender<(DataKey, RowData)>>| -> Result<()> {
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
                    match sender {
                        Some(sender) => sender.send((dataKey, rowData))?,
                        None => rowDatas.push((dataKey, rowData)),
                    }
                }

                Ok(())
            };

        // 要得到表的全部的data
        if dataKeys[0] == global::TOTAL_DATA_OF_TABLE {
            for dataKey in dataKeys[1]..=dataKeys[2] {
                processDataKey(dataKey, None)?;
            }
        } else {
            // todo 使用rayon 遍历
            for dataKey in dataKeys {
                processDataKey(*dataKey, None)?;
            }
        }

        Ok(rowDatas)
    }


    // todo 实现 index
    // 如果传递的是fn()的话(不是Fn)是函数指针而不是闭包 不能和上下文有联系 闭包返回false 那么 continue
    /// 目前用到hook的地点有 update() selectTableUnderRels()
    pub(super) fn scanSatisfiedRows<A, B, C, D>(&self,
                                                scanParams: ScanParams,
                                                select: bool,
                                                mut scanHooks: ScanHooks<A, B, C, D>) -> Result<Vec<(DataKey, RowData)>>
        where
            A: ScanCommittedPreProcessor,
            B: ScanCommittedPostProcessor,
            C: ScanUncommittedPreProcessor,
            D: ScanUncommittedPostProcessor,
    {

        // todo 使用table id 为 column family 标识
        let columnFamily = Session::getColFamily(&scanParams.table.name)?;

        let tableName_mutationsOnTable = self.session.tableName_mutations.read().unwrap();
        let tableMutationsCurrentTx: Option<&TableMutations> = tableName_mutationsOnTable.get(&scanParams.table.name);

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);

        let mut satisfiedRows =
            if scanParams.tableFilter.is_some() || select {
                let mut satisfiedRows = Vec::new();

                let mut serialScan = true;

                // 如果设置 scanConcurrency >1 说明是有 concurrent可能,到底是不是还要看下边的
                if self.session.scanConcurrency > 1 {
                    const COUNT_PER_THREAD: u64 = 2;

                    // todo 需要添加统计功能记录表有多少条data
                    // 计算需要多少 concurrency
                    let latestRowId = scanParams.table.rowIdCounter.load(Ordering::Acquire) - 1;
                    let distance = latestRowId - meta::ROW_ID_INVALID;
                    let mut concurrency = distance / COUNT_PER_THREAD;

                    if concurrency > 1 {
                        serialScan = false;

                        if concurrency > self.session.scanConcurrency as u64 {
                            concurrency = self.session.scanConcurrency as u64;
                        }

                        let rowCountPerThread = distance / concurrency;

                        // range的两边都是闭区间
                        // 以下是给各个thread使用itetate的range
                        let mut ranges: Vec<(DataKey, DataKey)> = Vec::with_capacity(concurrency as usize + 1);
                        let mut lastRoundEnd = meta::ROW_ID_INVALID;
                        for _ in 0..concurrency {
                            let start: RowId = lastRoundEnd + 1;
                            let end: RowId = start + rowCountPerThread;

                            ranges.push((keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, start), keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, end)));

                            lastRoundEnd = end;
                        }
                        // 不要忘了到末尾的tail
                        ranges.push((keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, lastRoundEnd + 1), keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, meta::ROW_ID_MAX)));

                        // let mut threadList = Vec::with_capacity(ranges.len());

                        // 以下是相当危险的,rust的引用直接转换成指针对应的数字,然后跨thread传递
                        // 能这么干的原因是,知道concurrent scan的涉及范围 会限制在当前函数之内 不会逃逸 因为后边要等待它们都结束
                        // 然而编译器是不知道这么细的细节的 只能1棒杀掉报错
                        let commandExecutorPointer = self as *const CommandExecutor as u64;
                        let scanHooksPointer = &mut scanHooks as *mut ScanHooks<A, B, C, D> as u64;
                        let tableFilterPointer =
                            match scanParams.tableFilter {
                                Some(expr) => Some(expr as *const Expr as u64),
                                None => None
                            };
                        let selectedColumnNamesPointer =
                            match scanParams.selectedColumnNames {
                                Some(selectedColumnNames) => Some(selectedColumnNames as *const Vec<String> as u64),
                                None => None
                            };

                        satisfiedRows = rayon::scope(move |scope| {
                            let (sender, receiver) = mpsc::sync_channel(COUNT_PER_THREAD as usize);

                            for (dataKeyStart, dataKeyEnd) in ranges {
                                let sender = sender.clone();
                                scope.spawn(move |scope| unsafe {
                                    // 要另外去包裹1层的原因是,通过sender发送的消息本身来传读相应的错误让外边知道
                                    let processor = || {
                                        let tableName = scanParams.table.name.clone();

                                        // 还原变为
                                        let commandExecutor: &CommandExecutor<'session> = mem::transmute(commandExecutorPointer as *const CommandExecutor);
                                        let tableFilter: Option<&Expr> = tableFilterPointer.map(|tableFilterPointer| mem::transmute(tableFilterPointer as *const Expr));
                                        let selectedColumnNames: Option<&Vec<String>> = selectedColumnNamesPointer.map(|selectedColumnNamesPointer| mem::transmute(selectedColumnNamesPointer as *const Vec<String>));
                                        let scanHooks: &mut ScanHooks<A, B, C, D> = mem::transmute(scanHooksPointer as *mut ScanHooks<A, B, C, D>);

                                        let table = commandExecutor.getTableRefByName(tableName.as_str())?;
                                        let snapshot = commandExecutor.session.getSnapshot()?;
                                        // column不是sync的 只能到thread上建立的
                                        let columnFamily = Session::getColFamily(tableName.as_str())?;

                                        let tableName_mutationsOnTable = commandExecutor.session.tableName_mutations.read().unwrap();
                                        let tableMutationsCurrentTx: Option<&TableMutations> = tableName_mutationsOnTable.get(table.name.as_str());

                                        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
                                        let mut rawIterator: DBRawIterator = snapshot.raw_iterator_cf(&columnFamily);

                                        // let mut rowDatas = Vec::new();
                                        let mut readCount = 0usize;

                                        for iterResult in snapshot.iterator_cf(&columnFamily, IteratorMode::From(u64ToByteArrRef!(dataKeyStart), Direction::Forward)) {
                                            let (dataKeyBinary, rowDataBinary) = iterResult?;

                                            let dataKey: DataKey = byte_slice_to_u64!(&*dataKeyBinary);

                                            if dataKey > dataKeyEnd {
                                                break;
                                            }

                                            // visibility
                                            if commandExecutor.checkCommittedDataVisibilityWithoutTxMutations(&mut mvccKeyBuffer,
                                                                                                              &mut rawIterator,
                                                                                                              dataKey,
                                                                                                              &columnFamily,
                                                                                                              &table.name)? == false {
                                                continue;
                                            }

                                            // visibility
                                            if let Some(mutationsRawCurrentTx) = tableMutationsCurrentTx {
                                                if commandExecutor.checkCommittedDataVisibilityWithTxMutations(mutationsRawCurrentTx,
                                                                                                               &mut mvccKeyBuffer, dataKey)? == false {
                                                    continue;
                                                }
                                            }

                                            // committed pre
                                            if let Some(ref mut scanCommittedPreProcessor) = scanHooks.scanCommittedPreProcessor {
                                                if scanCommittedPreProcessor(&columnFamily, dataKey)? == false {
                                                    continue;
                                                }
                                            }

                                            if let Some(rowData) = commandExecutor.readRowDataBinary(table.value(), &*rowDataBinary, tableFilter, selectedColumnNames)? {
                                                // committed post
                                                if let Some(ref mut scanCommittedPostProcessor) = scanHooks.scanCommittedPostProcessor {
                                                    if scanCommittedPostProcessor(&columnFamily, dataKey, &rowData)? == false {
                                                        continue;
                                                    }
                                                }

                                                // concurrent scan 当前不知如何应对offset 只能应对limit
                                                // 而且limit不能分割平分到各个thread上,需要各个thread都要收集的数量都要满足limit
                                                // 因可能这块没有 那块有
                                                if let Some(limit) = scanParams.limit {
                                                    if readCount >= limit {
                                                        break;
                                                    }
                                                }

                                                sender.send(Result::<(DataKey, RowData)>::Ok((dataKey, rowData))).expect("impossible");

                                                suffix_plus_plus!(readCount);
                                            }
                                        }

                                        Result::<()>::Ok(())
                                    };

                                    // 错误通过消息去传递 让外边及时知道
                                    match processor() {
                                        Ok(()) => {}
                                        Err(e) => sender.send(Result::<(DataKey, RowData)>::Err(e)).expect("impossible")
                                    }
                                });
                            }

                            // 不然的话下边的遍历receiver永远也不会return
                            mem::drop(sender);

                            for a in receiver {
                                let a = a?;
                                if let Some(limit) = scanParams.limit {
                                    if satisfiedRows.len() >= limit {
                                        break;
                                    }
                                }

                                satisfiedRows.push(a);
                            }

                            Result::<Vec<(DataKey, RowData)>>::Ok(satisfiedRows)
                        })?;
                    }
                }

                // 虽然设置了可以对线程scan 然而可能因为实际的数据量不够还是用不到
                if serialScan {
                    let snapshot = self.session.getSnapshot()?;

                    // mvcc的visibility筛选
                    let mut rawIterator: DBRawIterator = snapshot.raw_iterator_cf(&columnFamily);

                    let mut readCount = 0usize;

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
                                                                               &scanParams.table.name)? == false {
                            continue;
                        }

                        // 以上是全都在已落地的维度内的visibility check
                        // 还要结合当前事务上的尚未提交的mutations,看已落地的是不是应该干掉
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
                        if let Some(rowData) = self.readRowDataBinary(scanParams.table, &*rowDataBinary, scanParams.tableFilter, scanParams.selectedColumnNames)? {
                            if let Some(ref mut scanCommittedPostProcessor) = scanHooks.scanCommittedPostProcessor {
                                if scanCommittedPostProcessor(&columnFamily, dataKey, &rowData)? == false {
                                    continue;
                                }
                            }

                            // 应对 offset
                            if let Some(offset) = scanParams.offset {
                                if offset > readCount {
                                    continue;
                                }
                            }

                            // 应对 limit
                            if let Some(limit) = scanParams.limit {
                                if satisfiedRows.len() >= limit {
                                    break;
                                }
                            }

                            satisfiedRows.push((dataKey, rowData));

                            suffix_plus_plus!(readCount);
                        }
                    }
                }
                satisfiedRows
            } else { // 说明是link 且尚未写filter
                let mut rawIterator: DBRawIterator = self.session.getSnapshot()?.raw_iterator_cf(&columnFamily);
                rawIterator.seek(meta::DATA_KEY_PATTERN);

                if rawIterator.valid() == false {
                    Vec::new()
                } else {
                    // start include
                    let startKeyBinInclude = rawIterator.key().unwrap();
                    let startKeyInclude = byte_slice_to_u64!(startKeyBinInclude);

                    // end include
                    // seek_for_prev 意思是 定位到目标 要是目标没有的话 那么定位到它前个
                    rawIterator.seek_for_prev(u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, meta::ROW_ID_MAX)));
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

        // 然后看mutations里边的 有没有想要的
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

                if let Some(rowData) = self.readRowDataBinary(scanParams.table, addRowDataBinaryCurrentTx, scanParams.tableFilter, scanParams.selectedColumnNames)? {
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
                         rowBinary: &[Byte],
                         tableFilter: Option<&Expr>,
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

    pub(super) fn generateInsertValuesBinary(&self, insert: &mut Insert, table: &Table) -> Result<Bytes> {
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
                                       tEqT0: impl Fn(&T, &T0) -> bool) -> Vec<&'a T> where
            T: Clone + PartialEq,
            T0: Clone + PartialEq,
        {
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
        where
            A: CommittedPointerKeyProcessor,
            B: UncommittedPointerKeyProcessor,
    {
        let mut keys = Vec::new();

        let mut pointerKeyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

        let columnFamily = Session::getColFamily(tableName)?;

        let snapshot = self.session.getSnapshot()?;
        let mut rawIterator = snapshot.raw_iterator_cf(&columnFamily) as DBRawIterator;

        let tableName_mutationsOnTable = self.session.tableName_mutations.read().unwrap();
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





