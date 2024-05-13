use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use crate::config::CONFIG;
use crate::{command_executor, file_goto_start, global, prefix_plus_plus, suffix_plus_plus, throw};
use crate::meta::{Column, Table, TableType};
use crate::parser::{Command, Delete, Insert, Link, Select};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Map, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use bytes::{BufMut, BytesMut};
use lazy_static::lazy_static;
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
                        dataFile: None,
                        restore: table.restore,
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

        // 写wal buffer 写data buffer commit后wal buffer持久化
        Ok(())
    }

    async fn createTable(&self, mut table: Table) -> Result<()> {
        if global::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
            throw!(&format!("table/relation: {} already exist",table.name))
        }

        // 表对应的data 文件
        let tableDataFilePath = (CONFIG.dataDir.as_ref() as &Path).join(&table.name);

        if table.restore == false {
            if tableDataFilePath.exists() {
                throw!(&format!("data file of table:{} has already exist", table.name));
            }


            // table_record 文件
            let jsonString = serde_json::to_string(&table)?;

            {
                let option = &(**global::TABLE_RECORD_FILE.load());
                let mut tableRecordFile = option.as_ref().unwrap().write().await;
                tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r', b'\n']].concat().as_ref()).await?
            };
        }

        let mut tableDataFile = OpenOptions::new().write(true).read(true).create(true).open(tableDataFilePath.as_path()).await?;

        file_goto_start!(tableDataFile);

        table.dataFile = Some(tableDataFile);

        // map
        global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

        Ok(())
    }

    async fn insert(&self, insert: &Insert) -> Result<()> {
        // 对应的表是不是exist
        let mut table = self.getTableRefMutByName(&insert.tableName)?;

        // 不能对relation使用insert into
        if let TableType::Relation = table.type0 {
            throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insert.tableName));
        }

        let rowData = self.generateInsertValuesJson(&insert, &*table)?;
        let bytesMut = self.writeBytesMut(&rowData)?;
        table.dataFile.as_mut().unwrap().write_all(bytesMut.as_ref()).await?;

        Ok(())
    }

    /// 它本质是向relation对应的data file写入
    /// 两个元素之间的relation只看种类不看里边的属性的
    async fn link(&self, link: &Link) -> Result<()> {
        // 得到3个表的对象
        let mut srcTable = self.getTableRefMutByName(link.srcTableName.as_str())?;
        let mut destTable = self.getTableRefMutByName(link.destTableName.as_str())?;

        // 对src table和dest table调用expr筛选
        let srcSatisfiedPositions = self.scanSatisfiedRows(link.srcTableFilterExpr.as_ref(), srcTable.value_mut(), false, None).await?;
        // src 空的 link 不成立
        if srcSatisfiedPositions.is_empty() {
            return Ok(());
        }

        let destSatisfiedPositions = self.scanSatisfiedRows(link.destTableFilterExpr.as_ref(), destTable.value_mut(), false, None).await?;
        // dest 空的 link 不成立
        if destSatisfiedPositions.is_empty() {
            return Ok(());
        }

        // 用insetValues套路
        {
            let insertValues = Insert {
                tableName: link.relationName.clone(),
                useExplicitColumnNames: true,
                columnNames: link.relationColumnNames.clone(),
                columnExprs: link.relationColumnExprs.clone(),
            };
            let mut relationTable = self.getTableRefMutByName(link.relationName.as_str())?;
            let mut rowData = self.generateInsertValuesJson(&insertValues, &*relationTable)?;

            rowData["src"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:srcTable.name.clone(),
            positions:srcSatisfiedPositions.iter().map(|tuple|tuple.0).collect(),
        }));
            rowData["dest"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:destTable.name.clone(),
            positions:destSatisfiedPositions.iter().map(|tuple|tuple.0).collect(),
        }));

            let bytesMut = self.writeBytesMut(&rowData)?;

            let relationDataFile = relationTable.dataFile.as_mut().unwrap();
            // bytesMut.as_ref() 也可以使用 &bytesMut[..]
            relationDataFile.write_all(bytesMut.as_ref()).await?;
        }

        Ok(())
    }

    /// 如果不是含有relation的select 便是普通的select
    async fn select(&self, selectVec: &[Select]) -> Result<()> {
        // 普通模式不含有relation
        if selectVec.len() == 1 && selectVec[0].relationName.is_none() {
            let select = &selectVec[0];
            let mut srcTable = self.getTableRefMutByName(select.srcName.as_str())?;

            let rows = self.scanSatisfiedRows(select.srcFilterExpr.as_ref(), srcTable.value_mut(), true, select.srcColumnNames.as_ref()).await?;
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
        let mut destPositionsInPrevSelect: Option<Vec<DataPosition>> = None;

        // 1个select对应Vec<SelectResult> 多个select对应Vec<Vec<SelectResult>>
        let mut selectResultVecVec: Vec<Vec<SelectResult>> = Vec::with_capacity(selectVec.len());

        'loopSelect:
        for select in selectVec {
            // 为什么要使用{} 不然的话有概率死锁 https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
            let relationDatas: Vec<HashMap<String, GraphValue>> = {
                let mut relation = self.getTableRefMutByName(select.relationName.as_ref().unwrap())?;
                let relationDatas = self.scanSatisfiedRows(select.relationFliterExpr.as_ref(), relation.value_mut(), true, select.relationColumnNames.as_ref()).await?;
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
                    let mut srcTable = self.getTableRefMutByName(select.srcName.as_str())?;

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
                            self.getRowsByPositions(&intersect, &mut srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref()).await?
                        }
                        // 只会在首轮的
                        None => self.getRowsByPositions(&srcPointDesc.positions, &mut srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref()).await?,
                    }
                };
                if srcRowDatas.is_empty() {
                    continue;
                }

                let destRowDatas = {
                    let mut destTable = self.getTableRefMutByName(select.destName.as_ref().unwrap())?;
                    self.getRowsByPositions(&destPointDesc.positions, &mut destTable, select.destFilterExpr.as_ref(), select.destColumnNames.as_ref()).await?
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
        let mut table = self.getTableRefMutByName(delete.tableName.as_str())?;

        let pairs = self.scanSatisfiedRows(delete.filterExpr.as_ref(), table.value_mut(), true, None).await?;

        let tableDataFile = table.value_mut().dataFile.as_mut().unwrap();

        // 遍历更改的xmax
        for (dataPosition, _) in pairs {
            // 要更改的是xmax 在xmin后边
            // 之前发现即使seek到了正确的位置,写入还是到末尾append的 原因是openOptions设置了append
            tableDataFile.seek(SeekFrom::Start(dataPosition + global::TX_ID_LEN as u64)).await?;
            tableDataFile.write_u64(self.session.txId).await?;
        }

        Ok(())
    }

    /// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
    async fn getRowsByPositions(&self,
                                positions: &[DataPosition],
                                table: &mut Table,
                                tableFilterExpr: Option<&Expr>,
                                selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(DataPosition, RowData)>> {
        let tableDataFile = table.dataFile.as_mut().unwrap();

        let mut dataLenBuffer = [0; 8];

        // 要得到表的全部的data
        if positions[0] == global::TOTAL_DATA_OF_TABLE {
            self.scanSatisfiedRows(tableFilterExpr, table, true, selectedColumnNames).await
        } else {
            let mut rowDatas = Vec::with_capacity(positions.len());

            for position in positions {
                tableDataFile.seek(SeekFrom::Start(*position)).await?;
                if let (Some(rowData), _, _) = self.readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await? {
                    rowDatas.push((*position, rowData));
                }
            }

            Ok(rowDatas)
        }
    }

    /// 目标是普通表
    async fn scanSatisfiedRows(&self,
                               tableFilterExpr: Option<&Expr>,
                               table: &mut Table,
                               select: bool,
                               selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(DataPosition, RowData)>> {
        let tableDataFile = table.dataFile.as_mut().unwrap();

        tableDataFile.seek(SeekFrom::Start(0)).await?;

        let satisfiedRows =
            if tableFilterExpr.is_some() || select {
                let mut satisfiedRows = Vec::new();

                let mut dataLenBuffer = [0; 8];

                let mut position: DataPosition = 0;

                loop {
                    let (rowData, reachEnd, rowDataBinaryLen) = self.readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await?;
                    if reachEnd {
                        break;
                    }

                    if rowData.is_some() {
                        satisfiedRows.push((position, rowData.unwrap()));
                    }

                    position += rowDataBinaryLen as DataPosition;
                }

                satisfiedRows
            } else {
                vec![(global::TOTAL_DATA_OF_TABLE, HashMap::default())]
            };

        Ok(satisfiedRows)
    }

    /// 调用的前提是当前文件的位置到了row的start
    async fn readRow(&self,
                     tableDataFile: &mut File,
                     tableFilterExpr: Option<&Expr>,
                     selectedColumnNames: Option<&Vec<String>>,
                     dataLenBuffer: &mut [u8; 8]) -> Result<(Option<RowData>, ReachEnd, DataLen)> {
        // 相当的坑 有read()和read_buf() 前边看的是len后边看的是capactiy
        // 后边的是不能用的 虽然有Vec::with_capacity() 然而随读取的越多vec本身也会扩容的 后来改为 [0; global::ROW_DATA_LEN_FIELD_LEN]

        macro_rules! expand {
            ($dataLenBuffer: expr) => {
                [$dataLenBuffer[0], $dataLenBuffer[1], $dataLenBuffer[2], $dataLenBuffer[3], $dataLenBuffer[4], $dataLenBuffer[5], $dataLenBuffer[6], $dataLenBuffer[7]]
            };
        }
        // xmin
        let readCount = tableDataFile.read(dataLenBuffer).await?;
        if readCount == 0 {  // reach the end
            return Ok((None, true, 0));
        }
        assert_eq!(readCount, global::TX_ID_LEN);
        let xmin: TxId = TxId::from_be_bytes(expand!(dataLenBuffer));

        // xmax
        let readCount = tableDataFile.read(dataLenBuffer).await?;
        if readCount == 0 {  // reach the end
            return Ok((None, true, 0));
        }
        assert_eq!(readCount, global::TX_ID_LEN);
        let xmax: TxId = TxId::from_be_bytes(expand!(dataLenBuffer));

        // next valid position
        let readCount = tableDataFile.read(dataLenBuffer).await?;
        if readCount == 0 {  // reach the end
            return Ok((None, true, 0));
        }
        assert_eq!(readCount, global::ROW_NEXT_POSITION_LEN);
        let nextPosition: DataPosition = DataPosition::from_be_bytes(expand!(dataLenBuffer));

        // 读取content
        let readCount = tableDataFile.read(&mut dataLenBuffer[0..global::ROW_CONTENT_LEN_FIELD_LEN]).await?;
        if readCount == 0 {  // reach the end
            return Ok((None, true, 0));
        }
        assert_eq!(readCount, global::ROW_CONTENT_LEN_FIELD_LEN);
        let contentLen = u32::from_be_bytes([dataLenBuffer[0], dataLenBuffer[1], dataLenBuffer[2], dataLenBuffer[3]]) as usize;
        let rowTotalLen: DataLen = (global::ROW_PREFIX_LEN + contentLen) as DataLen;

        // 不在可视范围里边
        if xmin > self.session.txId || (xmax != global::TX_ID_INVALID && self.session.txId >= xmax) {
            return Ok((None, false, rowTotalLen));
        }

        let mut dataBuffer = vec![0u8; contentLen];
        let readCount = tableDataFile.read(&mut dataBuffer).await?;
        assert_eq!(readCount, contentLen);
        let jsonString = String::from_utf8(dataBuffer)?;

        let mut rowData: HashMap<String, GraphValue> = serde_json::from_str(&jsonString)?;

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
            return Ok((Some(rowData), false, rowTotalLen));
        }

        if let GraphValue::Boolean(satisfy) = tableFilterExpr.unwrap().calc(Some(&rowData))? {
            if satisfy {
                Ok((Some(rowData), false, rowTotalLen))
            } else {
                Ok((None, false, rowTotalLen))
            }
        } else {
            throw!("table filter should get a boolean")
        }
    }

    fn getTableRefMutByName(&self, tableName: &str) -> Result<RefMut<String, Table>> {
        let table = global::TABLE_NAME_TABLE.get_mut(tableName);
        if table.is_none() {
            throw!(&format!("table:{} not exist", tableName));
        }
        Ok(table.unwrap())
    }

    fn generateInsertValuesJson(&self, insert: &Insert, table: &Table) -> Result<Value> {
        let columns = {
            let mut columns = Vec::new();

            // 要是未显式说明column的话还需要读取table的column
            if insert.useExplicitColumnNames == false {
                for column in &table.columns {
                    columns.push(column);
                }
            } else { // 如果显式说明columnName的话需要确保都是有的
                for columnName in &insert.columnNames {
                    let mut found = false;

                    for column in &table.columns {
                        if columnName == &column.name {
                            columns.push(column);
                            found = true;
                            break;
                        }
                    }

                    if found == false {
                        throw!(&format!("column {} does not defined", columnName));
                    }
                }
            }

            columns
        };

        // 确保column数量和value数量相同
        if columns.len() != insert.columnExprs.len() {
            throw!("column count does not match value count");
        }

        let mut rowData = json!({});

        for column_columnValue in columns.iter().zip(insert.columnExprs.iter()) {
            let column = column_columnValue.0;
            let columnExpr = column_columnValue.1;

            // columnType和value也要对上
            let columnValue = columnExpr.calc(None)?;
            if column.type0.compatible(&columnValue) == false {
                throw!(&format!("column:{},type:{} is not compatible with value:{}", column.name, column.type0, columnValue));
            }

            rowData[column.name.as_str()] = json!(columnValue);
        }

        Ok(rowData)
    }

    /// 8byte tx_min + 8byte tx_max + 8byte next valid position + 4byte conetnt len
    fn writeBytesMut(&self, rowData: &Value) -> Result<BytesMut> {
        let jsonString = serde_json::to_string(rowData)?;

        let jsonStringByte = jsonString.as_bytes();
        let dataLen = jsonStringByte.len();

        assert!(u32::MAX as usize >= jsonStringByte.len());

        let mut bytesMut = BytesMut::with_capacity(global::ROW_PREFIX_LEN + jsonStringByte.len());

        // tx_min
        bytesMut.put_u64(self.session.txId);

        // tx_max
        bytesMut.put_u64(global::TX_ID_INVALID);

        // next valid position
        bytesMut.put_u64(0);

        // content len
        bytesMut.put_u32(dataLen as u32);

        // conetent
        bytesMut.put_slice(jsonStringByte);

        Ok(bytesMut)
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
    use crate::{global, meta, parser};
    use crate::command_executor;
    use crate::meta::TABLE_RECORD_FILE_NAME;
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

        let a: A = serde_json::from_str("{\"S\":\"1\"}").unwrap();
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
}
