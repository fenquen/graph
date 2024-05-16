use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::config::CONFIG;
use crate::{byte_slice_to_u64, command_executor, file_goto_start, global, meta, prefix_plus_plus, suffix_plus_plus, throw, u64_to_byte_slice};
use crate::meta::{Column, ColumnType, RowId, Table, TableId, TableType};
use crate::parser::{Command, Delete, Element, Insert, Link, Select, Update};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Map, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use bytes::{BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use rocksdb::{IteratorMode, Options};
use crate::codec::{BinaryCodec, MyBytes};
use crate::expr::Expr;
use crate::global::{ReachEnd, DataLen, DataPosition, TX_ID_COUNTER, TxId};
use crate::graph_value::{GraphValue, PointDesc};
use crate::session::Session;

type RowData = HashMap<String, GraphValue>;

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

pub struct CommandExecutor<'sessionLife> {
    pub session: &'sessionLife Session,
}

impl<'sessionLife> CommandExecutor<'sessionLife> {
    pub fn new(session: &'sessionLife Session) -> Self {
        CommandExecutor {
            session
        }
    }

    pub async fn execute(&self, commands: &[Command]) -> Result<()> {
        for command in commands {
            match command {
                Command::CreateTable(table) => {
                    let table = Table {
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        type0: table.type0.clone(),
                        rowIdCounter: AtomicU64::default(),
                        tableId: TableId::default(),
                    };

                    self.createTable(table).await?;
                }
                Command::Insert(insertValues) => self.insert(insertValues).await?,
                Command::Select(select) => self.select(select).await?,
                Command::Link(link) => self.link(link).await?,
                Command::Delete(delete) => self.delete(delete).await?,
                _ => throw!(&format!("unsupported command:{:?}", command)),
            }
        }

        Ok(())
    }

    async fn createTable(&self, mut table: Table) -> Result<()> {
        if meta::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
            throw!(&format!("table/relation: {} already exist", table.name))
        }

        // 如果是relation 系统内部额外添加2个PointDesc的内部字段 src dest
        if let TableType::Relation = table.type0 {
            table.columns.push(
                Column {
                    name: "src".to_string(),
                    type0: ColumnType::PointDesc,
                }
            );

            table.columns.push(
                Column {
                    name: "dest".to_string(),
                    type0: ColumnType::PointDesc,
                }
            );
        }

        table.tableId = meta::TABLE_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // 生成column family
        self.session.createColFamily(table.name.as_str())?;

        // todo 使用 u64的tableId 为key
        let key = u64_to_byte_slice!(table.tableId);
        let json = serde_json::to_string(&table)?;
        meta::STORE.meta.put(key, json.as_bytes())?;

        // map
        meta::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

        Ok(())
    }

    // todo insert时候value的排布要和创建表的时候column的顺序对应
    async fn insert(&self, insert: &Insert) -> Result<()> {
        // 对应的表是不是exist
        let table = self.getTableRefByName(&insert.tableName)?;

        // 不能对relation使用insert into
        if let TableType::Relation = table.type0 {
            throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insert.tableName));
        }

        let rowDataBinary = self.generateInsertValuesBinary(&insert, &*table)?;

        let rowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let key = u64_to_byte_slice!(rowId);

        let value = rowDataBinary.as_ref();

        let columnFamily = self.session.getColFamily(&table.name)?;

        self.session.getCurrentTx()?.put_cf(&columnFamily, key, value)?;

        Ok(())
    }

    /// 它本质是向relation对应的data file写入
    /// 两个元素之间的relation只看种类不看里边的属性的
    async fn link(&self, link: &Link) -> Result<()> {
        // 得到3个表的对象
        let srcTable = self.getTableRefByName(link.srcTableName.as_str())?;
        let destTable = self.getTableRefByName(link.destTableName.as_str())?;

        // 对src table和dest table调用expr筛选
        let srcSatisfiedPositions = self.scanSatisfiedRowsBinary(link.srcTableFilterExpr.as_ref(), srcTable.value(), false, None)?;
        // src 空的 link 不成立
        if srcSatisfiedPositions.is_empty() {
            return Ok(());
        }

        let destSatisfiedPositions = self.scanSatisfiedRowsBinary(link.destTableFilterExpr.as_ref(), destTable.value(), false, None)?;
        // dest 空的 link 不成立
        if destSatisfiedPositions.is_empty() {
            return Ok(());
        }

        // 用insetValues套路
        {
            let mut insertValues = Insert {
                tableName: link.relationName.clone(),
                useExplicitColumnNames: true,
                columnNames: link.relationColumnNames.clone(),
                columnExprs: link.relationColumnExprs.clone(),
            };

            insertValues.columnNames.push("src".to_string());
            insertValues.columnExprs.push(Expr::Single(Element::PointDesc(
                PointDesc {
                    tableName: srcTable.name.clone(),
                    positions: srcSatisfiedPositions.iter().map(|tuple| tuple.0).collect(),
                }
            )));

            insertValues.columnNames.push("dest".to_string());
            insertValues.columnExprs.push(Expr::Single(Element::PointDesc(
                PointDesc {
                    tableName: destTable.name.clone(),
                    positions: destSatisfiedPositions.iter().map(|tuple| tuple.0).collect(),
                }
            )));

            let relationTable = self.getTableRefByName(link.relationName.as_str())?;

            let rowId: RowId = relationTable.rowIdCounter.fetch_add(1, Ordering::AcqRel);
            let key = u64_to_byte_slice!(rowId);

            let rowDataBinary = self.generateInsertValuesBinary(&insertValues, &*relationTable)?;
            let value = rowDataBinary.as_ref();

            let columnFamily = self.session.getColFamily(relationTable.name.as_str())?;

            self.session.getCurrentTx()?.put_cf(&columnFamily, key, value)?;
        }

        Ok(())
    }

    /// 如果不是含有relation的select 便是普通的select
    async fn select(&self, selectVec: &[Select]) -> Result<()> {
        // 普通模式不含有relation
        if selectVec.len() == 1 && selectVec[0].relationName.is_none() {
            let select = &selectVec[0];
            let srcTable = self.getTableRefByName(select.srcName.as_str())?;

            let rows = self.scanSatisfiedRowsBinary(select.srcFilterExpr.as_ref(), srcTable.value(), true, select.srcColumnNames.as_ref())?;
            let rows: Vec<RowData> = rows.into_iter().map(|tuple| tuple.1).collect();
            JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&rows)?));

            return Ok(());
        }

        // 对应1个realtion的query的多个条目的1个
        #[derive(Debug)]
        struct SelectResult {
            srcName: String,
            srcRowDatas: Vec<(DataPosition, RowData)>,
            relationName: String,
            relationData: RowData,
            destName: String,
            destRowDatas: Vec<(DataPosition, RowData)>,
        }

        // 给next轮用的
        let mut destPositionsInPrevSelect: Option<Vec<DataPosition>> = Default::default();

        // 1个select对应Vec<SelectResult> 多个select对应Vec<Vec<SelectResult>>
        let mut selectResultVecVec: Vec<Vec<SelectResult>> = Vec::with_capacity(selectVec.len());

        'loopSelect:
        for select in selectVec {
            // 为什么要使用{} 不然的话有概率死锁 https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
            let relationDatas: Vec<RowData> = {
                let relation = self.getTableRefByName(select.relationName.as_ref().unwrap())?;
                let relationDatas = self.scanSatisfiedRowsBinary(select.relationFliterExpr.as_ref(), relation.value(), true, select.relationColumnNames.as_ref())?;
                relationDatas.into_iter().map(|tuple| tuple.1).collect()
            };

            let mut selectResultVecInCurrentSelect = Vec::with_capacity(relationDatas.len());

            let mut destPositionsInCurrentSelect = vec![];

            // 遍历当前的select的多个relation
            'loopRelationData:
            for relationData in relationDatas {
                let srcPointDesc = relationData.get(PointDesc::SRC).unwrap().asPointDesc()?;
                // relation的src表的name不符合
                if srcPointDesc.tableName != select.srcName || srcPointDesc.positions.is_empty() {
                    continue;
                }

                // relation的dest表的name不符合
                let destPointDesc = relationData.get(PointDesc::DEST).unwrap().asPointDesc()?;
                if destPointDesc.tableName != (*select.destName.as_ref().unwrap()) || destPointDesc.positions.is_empty() {
                    continue;
                }

                let srcRowDatas = {
                    let srcTable = self.getTableRefByName(select.srcName.as_str())?;

                    // 上轮的全部的多个条目里边的dest的position 和 当前条目的src的position的交集
                    match destPositionsInPrevSelect {
                        Some(ref destPositionsInPrevSelect) => {
                            let intersect =
                                destPositionsInPrevSelect.iter().filter(|&&destPositionInPrevSelect| srcPointDesc.positions.contains(&destPositionInPrevSelect)).map(|a| *a).collect::<Vec<_>>();

                            // 说明 当前的这个relation的src和上轮的dest没有重合的
                            if intersect.is_empty() {
                                continue 'loopRelationData;
                            }

                            // 当前的select的src确定了 还要回去修改上轮的dest
                            if let Some(prevSelectResultVec) = selectResultVecVec.last_mut() {

                                // 遍历上轮的各个result的dest,把intersect之外的去掉
                                for prevSelectResult in &mut *prevSelectResultVec {
                                    // https://blog.csdn.net/u011528645/article/details/123117829
                                    prevSelectResult.destRowDatas.retain(|pair| intersect.contains(&pair.0));
                                }

                                // destRowDatas是空的话那么把selectResult去掉
                                prevSelectResultVec.retain(|prevSelectResult| prevSelectResult.destRowDatas.len() > 0);

                                // 连线断掉
                                if prevSelectResultVec.is_empty() {
                                    break 'loopSelect;
                                }
                            }

                            // 当前的使用intersect为源头
                            self.getRowsByPosBinary(&intersect, &*srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref())?
                        }
                        // 只会在首轮的
                        None => self.getRowsByPosBinary(&srcPointDesc.positions, &*srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref())?,
                    }
                };
                if srcRowDatas.is_empty() {
                    continue;
                }

                let destRowDatas = {
                    let destTable = self.getTableRefByName(select.destName.as_ref().unwrap())?;
                    self.getRowsByPosBinary(&destPointDesc.positions, &*destTable, select.destFilterExpr.as_ref(), select.destColumnNames.as_ref())?
                };
                if destRowDatas.is_empty() {
                    continue;
                }

                for destPosition in &destPointDesc.positions {
                    destPositionsInCurrentSelect.push(*destPosition);
                }

                selectResultVecInCurrentSelect.push(
                    SelectResult {
                        srcName: select.srcAlias.as_ref().unwrap_or_else(|| &select.srcName).to_string(),
                        srcRowDatas,
                        relationName: select.relationAlias.as_ref().unwrap_or_else(|| select.relationName.as_ref().unwrap()).to_string(),
                        relationData,
                        destName: select.destAlias.as_ref().unwrap_or_else(|| select.destName.as_ref().unwrap()).to_string(),
                        destRowDatas,
                    }
                );
            }

            destPositionsInPrevSelect = {
                // 当前的relation select 的多个realtion对应dest全都是empty的
                if destPositionsInCurrentSelect.is_empty() {
                    break 'loopSelect;
                }

                // rust的这个去重有点不同只能去掉连续的重复的 故而需要先排序让重复的连续起来
                destPositionsInCurrentSelect.sort();
                destPositionsInCurrentSelect.dedup();

                Some(destPositionsInCurrentSelect)
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
                let srcRowDatas: Vec<&RowData> = selectResult.srcRowDatas.iter().map(|pair| &pair.1).collect();
                let destRowDatas: Vec<&RowData> = selectResult.destRowDatas.iter().map(|pair| &pair.1).collect::<Vec<&RowData>>();

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
                        json[selectResult.destName.as_str()] = json!(selectResult.destRowDatas);
                    }
                }

                valueVec.push(json);
            }

            valueVec
        }

        let valueVec = JSON_ENUM_UNTAGGED!(handleResult(selectResultVecVec));
        println!("{}", serde_json::to_string(&valueVec)?);

        Ok(())
    }

    /// 得到满足expr的record 然后把它的xmax变为当前的txId
    async fn delete(&self, delete: &Delete) -> Result<()> {
        let pairs = {
            let table = self.getTableRefByName(delete.tableName.as_str())?;
            self.scanSatisfiedRowsBinary(delete.filterExpr.as_ref(), table.value(), true, None)?
        };

        let columnFamily = self.session.getColFamily(&delete.tableName)?;

        // 遍历更改的xmax
        for (dataPosition, _) in pairs {
            // 要更改的是xmax 在xmin后边
            // 之前发现即使seek到了正确的位置,写入还是到末尾append的 原因是openOptions设置了append
            self.session.getCurrentTx()?.delete_cf(&columnFamily, u64_to_byte_slice!(dataPosition))?;
        }

        Ok(())
    }

    fn update(&self, update: &Update) -> Result<()> {
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

        let mut pairs = self.scanSatisfiedRowsBinary(update.filterExpr.as_ref(), table.value(), true, None)?;

        enum A<'a> {
            DirectValue(GraphValue),
            NeedCalc(&'a Expr),
        }

        let mut columnName_a: HashMap<String, A> = HashMap::with_capacity(update.columnName_expr.len());

        macro_rules! column_type_compatible {
            ($columnName_column:expr, $columnName: expr,$columnValue: expr,$tableName: expr) => {
                match $columnName_column.get($columnName) {
                    Some(column) => {
                        if column.type0.compatible(&$columnValue) == false {
                            throw!(&format!("table:{} , column:{}, is not compatilbe with value:{:?}", $tableName, $columnName, $columnValue));
                        }
                    }
                    None => throw!(&format!("table:{} has no column named:{}", $tableName, $columnName)),
                }
            };
        }

        // column expr能直接计算的先计算 不要到后边的遍历里边重复计算了
        for (columnName, columnExpr) in &update.columnName_expr {
            if columnExpr.needAcutalRowData() {
                columnName_a.insert(columnName.to_string(), A::NeedCalc(columnExpr));
            } else {
                let columnValue = columnExpr.calc(None)?;

                // update设置的值要和column type 相同
                column_type_compatible!(columnName_column, columnName, columnValue, update.tableName);

                columnName_a.insert(columnName.to_string(), A::DirectValue(columnExpr.calc(None)?));
            }
        }

        let columnFamily = self.session.getColFamily(update.tableName.as_str())?;

        // todo update的时候能不能直接从binary维度上更改row
        for (rowId, rowData) in &mut pairs {
            for (columnName, a) in &columnName_a {
                match a {
                    A::NeedCalc(expr) => {
                        let columnValue = expr.calc(Some(rowData))?;

                        // update设置的值要和column type 相同
                        column_type_compatible!(columnName_column, columnName, columnValue, update.tableName);

                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                    A::DirectValue(columnValue) => {
                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                }
            }

            // todo 各个column的value都在1道使得update时候只能整体来弄太耦合了 后续设想能不能各个column保存到单独的key
            let key = u64_to_byte_slice!(*rowId);

            let destByteSlice = {
                let mut destByteSlice = BytesMut::new();
                // 需要以表定义里边的column顺序来序列化
                for column in &table.columns {
                    let columnValue = rowData.get(&column.name).unwrap();
                    columnValue.encode(&mut destByteSlice)?;
                }

                destByteSlice.freeze()
            };
            let value = destByteSlice.as_ref();

            self.session.getCurrentTx()?.put_cf(&columnFamily, key, value)?;
        }

        Ok(())
    }

    /// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
    fn getRowsByPosBinary(&self,
                          positions: &[DataPosition],
                          table: &Table,
                          tableFilter: Option<&Expr>,
                          selectedColNames: Option<&Vec<String>>) -> Result<Vec<(DataPosition, RowData)>> {
        // 要得到表的全部的data
        if positions[0] == global::TOTAL_DATA_OF_TABLE {
            self.scanSatisfiedRowsBinary(tableFilter, table, true, selectedColNames)
        } else {
            let mut rowDatas = Vec::with_capacity(positions.len());

            let columnFamily = meta::STORE.data.cf_handle(table.name.as_str()).unwrap();

            for position in positions {
                let rowBinary = self.session.getCurrentTx()?.get_cf(&columnFamily, u64_to_byte_slice!(*position))?.unwrap();

                if let Some(rowData) = self.readRowBinary(table, Box::from(&rowBinary[..]), tableFilter, selectedColNames)? {
                    rowDatas.push((*position, rowData));
                }
            }

            Ok(rowDatas)
        }
    }

    // todo 减少对table引用的持有的必要 让dashMap快速unlock
    fn scanSatisfiedRowsBinary(&self,
                               tableFilterExpr: Option<&Expr>,
                               table: &Table,
                               select: bool,
                               selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(DataPosition, RowData)>> {
        let columnFamily = self.session.getColFamily(table.name.as_str())?;
        let iterator = self.session.getCurrentTx()?.iterator_cf(&columnFamily, IteratorMode::Start);

        let satisfiedRows =
            if tableFilterExpr.is_some() || select {
                let mut satisfiedRows = Vec::new();

                for iterResult in iterator {
                    let pair = iterResult?;
                    //  let rowBinary = &*pair.1;
                    match self.readRowBinary(table, pair.1, tableFilterExpr, selectedColumnNames)? {
                        None => continue,
                        Some(rowData) => {
                            satisfiedRows.push((byte_slice_to_u64!(&*pair.0), rowData))
                        }
                    }
                }

                satisfiedRows
            } else {
                vec![(global::TOTAL_DATA_OF_TABLE, HashMap::default())]
            };

        Ok(satisfiedRows)
    }

    fn readRowBinary(&self,
                     table: &Table,
                     rowBinary: Box<[u8]>,
                     tableFilterExpr: Option<&Expr>,
                     selectedColumnNames: Option<&Vec<String>>) -> Result<Option<RowData>> {
        let columnNames = table.columns.iter().map(|column| column.name.clone()).collect::<Vec<String>>();

        let mut myBytesRowData = MyBytes::from(Bytes::from(rowBinary));
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

    fn generateInsertValuesBinary(&self, insert: &Insert, table: &Table) -> Result<Bytes> {
        let columns = {
            let mut columns = Vec::new();

            // 要是未显式说明column的话还需要读取table的column
            if insert.useExplicitColumnNames == false {
                for column in &table.columns {
                    columns.push(column);
                }
            } else { // 如果显式说明columnName的话需要确保都是有的
                for columnNameToInsert in &insert.columnNames {
                    let mut found = false;

                    for column in &table.columns {
                        if columnNameToInsert == &column.name {
                            columns.push(column);
                            found = true;
                            break;
                        }
                    }

                    if found == false {
                        throw!(&format!("column {} does not defined", columnNameToInsert));
                    }
                }
            }

            columns
        };

        // todo insert时候需要各column全都insert 后续要能支持 null的 GraphValue
        // 确保column数量和value数量相同
        if columns.len() != insert.columnExprs.len() || table.columns.len() != insert.columnExprs.len() {
            throw!("column count does not match value count");
        }

        // todo 如果指明了要insert的column name的话 需要排序 符合表定义时候的column顺序 完成
        let destByteSlice = {
            let mut columnName_columnExpr = HashMap::with_capacity(columns.len());
            for (column, columnExpr) in columns.iter().zip(insert.columnExprs.iter()) {
                columnName_columnExpr.insert(column.name.to_owned(), columnExpr);
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
}


#[cfg(test)]
mod test {
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use std::io::{SeekFrom, Write};
    use dashmap::DashMap;
    use serde::{Deserialize, Serialize, Serializer};
    use serde::ser::{SerializeMap, SerializeStruct};
    use serde_json::json;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use crate::graph_value::GraphValue;
    use crate::{byte_slice_to_u64, global, meta, parser, u64_to_byte_slice};
    use crate::command_executor;
    use crate::parser::Command;

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
        let s = u64_to_byte_slice!(2147389121 as u64);

        let s1 = u64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
        let aa = byte_slice_to_u64!(s);

        println!("{},{}", s1, aa);
    }
}
