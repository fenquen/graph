use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use crate::config::CONFIG;
use crate::{command_executor, global, prefix_plus_plus, suffix_plus_plus, throw};
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
use crate::global::{ReachEnd, RowContentBinaryLen, RowDataPosition};
use crate::graph_value::{GraphValue, PointDesc};

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

pub async fn execute(commands: Vec<Command>) -> Result<()> {
    for command in commands {
        match command {
            Command::CreateTable(table) => createTable(table).await?,
            Command::Insert(ref insertValues) => insert(insertValues).await?,
            Command::Select(ref select) => command_executor::select(select).await?,
            Command::Link(ref link) => command_executor::link(link).await?,
            Command::Delete(ref delete) => command_executor::delete(delete).await?,
            _ => throw!(&format!("unsupported command:{:?}", command)),
        }
    }

    // 写wal buffer 写data buffer commit后wal buffer持久化
    Ok(())
}

async fn writeWal() -> Result<()> {

    Ok(())
}

async fn createTable(mut table: Table) -> Result<()> {
    if global::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
        throw!(&format!("table/relation: {} already exist",table.name))
    }

    // 表对应的data 文件
    let tableDataFilePath = (CONFIG.dataDir.as_ref() as &Path).join(&table.name);

    if table.restore == false {
        if tableDataFilePath.exists() {
            throw!(&format!("data file of table:{} has already exist", table.name));
        }

        File::create(tableDataFilePath.as_path()).await?;

        // table_record 文件
        let jsonString = serde_json::to_string(&table)?;

        {
            let option = &(**global::TABLE_RECORD_FILE.load());
            let mut tableRecordFile = option.as_ref().unwrap().write().await;
            tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r', b'\n']].concat().as_ref()).await?
        };
    }

    let tableDataFile = OpenOptions::new().write(true).read(true).create(true).append(true).open(tableDataFilePath.as_path()).await?;
    table.dataFile = Some(tableDataFile);

    // map
    global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

    Ok(())
}

async fn insert(insert: &Insert) -> Result<()> {
    // 对应的表是不是exist
    let mut table = getTableRefMutByName(&insert.tableName)?;

    // 不能对relation使用insert into
    if let TableType::Relation = table.type0 {
        throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insert.tableName));
    }

    let rowData = generateInsertValuesJson(&insert, &*table)?;
    let bytesMut = writeBytesMut(&rowData)?;
    table.dataFile.as_mut().unwrap().write_all(bytesMut.as_ref()).await?;

    Ok(())
}

/// 它本质是向relation对应的data file写入
/// 两个元素之间的relation只看种类不看里边的属性的
async fn link(link: &Link) -> Result<()> {
    // 得到3个表的对象
    let mut srcTable = getTableRefMutByName(link.srcTableName.as_str())?;
    let mut destTable = getTableRefMutByName(link.destTableName.as_str())?;

    // 对src table和dest table调用expr筛选
    let srcSatisfiedPositions = scanSatisfiedRows(link.srcTableFilterExpr.as_ref(), srcTable.value_mut(), false, None).await?;
    // src 空的 link 不成立
    if srcSatisfiedPositions.is_empty() {
        return Ok(());
    }

    let destSatisfiedPositions = scanSatisfiedRows(link.destTableFilterExpr.as_ref(), destTable.value_mut(), false, None).await?;
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
        let mut relationTable = getTableRefMutByName(link.relationName.as_str())?;
        let mut rowData = generateInsertValuesJson(&insertValues, &*relationTable)?;

        rowData["src"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:srcTable.name.clone(),
            positions:srcSatisfiedPositions.iter().map(|tuple|tuple.0).collect(),
        }));
        rowData["dest"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:destTable.name.clone(),
            positions:destSatisfiedPositions.iter().map(|tuple|tuple.0).collect(),
        }));

        let bytesMut = writeBytesMut(&rowData)?;

        let relationDataFile = relationTable.dataFile.as_mut().unwrap();
        // bytesMut.as_ref() 也可以使用 &bytesMut[..]
        relationDataFile.write_all(bytesMut.as_ref()).await?;
    }

    Ok(())
}

/// 如果不是含有relation的select 便是普通的select
async fn select(selectVec: &[Select]) -> Result<()> {
    // 普通模式不含有relation
    if selectVec.len() == 1 && selectVec[0].relationName.is_none() {
        let select = &selectVec[0];
        let mut srcTable = getTableRefMutByName(select.srcName.as_str())?;

        let rows = scanSatisfiedRows(select.srcFilterExpr.as_ref(), srcTable.value_mut(), true, select.srcColumnNames.as_ref()).await?;
        let rows: Vec<RowData> = rows.into_iter().map(|tuple| tuple.1).collect();
        JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&rows)?));

        return Ok(());
    }

    // 对应1个realtion的query的多个条目的1个
    #[derive(Debug)]
    struct SelectResult {
        srcName: String,
        srcRowDatas: Vec<(RowDataPosition, RowData)>,
        relationName: String,
        relationData: RowData,
        destName: String,
        destRowDatas: Vec<(RowDataPosition, RowData)>,
    }

    // 给next轮用的
    let mut destPositionsInPrevSelect: Option<Vec<RowDataPosition>> = None;

    // 1个select对应Vec<SelectResult> 多个select对应Vec<Vec<SelectResult>>
    let mut selectVecResultVecVec: Vec<Vec<SelectResult>> = Vec::with_capacity(selectVec.len());

    'loopSelect:
    for select in selectVec {
        // 为什么要使用{} 不然的话有概率死锁 https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
        let relationDatas: Vec<HashMap<String, GraphValue>> = {
            let mut relation = getTableRefMutByName(select.relationName.as_ref().unwrap())?;
            let relationDatas = scanSatisfiedRows(select.relationFliterExpr.as_ref(), relation.value_mut(), true, select.relationColumnNames.as_ref()).await?;
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
                let mut srcTable = getTableRefMutByName(select.srcName.as_str())?;

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
                        if let Some(prevSelectResultVec) = selectVecResultVecVec.last_mut() {

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
                        getRowsByPositions(&intersect, &mut srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref()).await?
                    }
                    // 只会在首轮的
                    None => getRowsByPositions(&srcPointDesc.positions, &mut srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref()).await?,
                }
            };
            if srcRowDatas.is_empty() {
                continue;
            }

            let destRowDatas = {
                let mut destTable = getTableRefMutByName(select.destName.as_ref().unwrap())?;
                getRowsByPositions(&destPointDesc.positions, &mut destTable, select.destFilterExpr.as_ref(), select.destColumnNames.as_ref()).await?
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

        selectVecResultVecVec.push(selectResultVecInCurrentSelect);
    }

    /// ```[[[第1个select的第1行data],[第1个select的第2行data]],[[第2个select的第1行data],[第2个select的第2行data]]]```
    /// 到时候要生成4条脉络
    fn handleResult(selectVecResultVecVec: Vec<Vec<SelectResult>>) -> Vec<Value> {
        let mut valueVec = Vec::default();

        for selectResult in &selectVecResultVecVec[0] {
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
                if outerIndex == selectVecResultVecVec.len() {
                    break;
                }

                for selectResult in selectVecResultVecVec.get(outerIndex).unwrap() {
                    json[selectResult.relationName.as_str()] = json!(selectResult.relationData);
                    json[selectResult.destName.as_str()] = json!(selectResult.destRowDatas);
                }
            }

            valueVec.push(json);
        }

        valueVec
    }

    let valueVec = JSON_ENUM_UNTAGGED!(handleResult(selectVecResultVecVec));
    println!("{}", serde_json::to_string(&valueVec)?);

    Ok(())
}

async fn delete(delete: &Delete) -> Result<()> {
    let targetTable = getTableRefMutByName(delete.tableName.as_str())?;

    // 对相应的data打上标识

    Ok(())
}

/// 目前使用的场合是通过realtion保存的两边node的position得到相应的node
async fn getRowsByPositions(positions: &[RowDataPosition],
                            table: &mut Table,
                            tableFilterExpr: Option<&Expr>,
                            selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(RowDataPosition, RowData)>> {
    let tableDataFile = table.dataFile.as_mut().unwrap();

    let mut dataLenBuffer = [0; 8];

    // 要得到表的全部的data
    let rowDatas =
        if positions[0] == global::TOTAL_DATA_OF_TABLE {
            tableDataFile.seek(SeekFrom::Start(0)).await?;

            let mut rowDatas = Vec::default();

            let mut position: RowDataPosition = 0;
            loop {
                if let (Some(rowData), _, rowDataBinaryLen) = readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await? {
                    position += rowDataBinaryLen as RowDataPosition;
                    rowDatas.push((position, rowData));
                }
                break;
            }

            rowDatas
        } else {
            let mut rowDatas = Vec::with_capacity(positions.len());

            for position in positions {
                tableDataFile.seek(SeekFrom::Start(*position)).await?;
                if let (Some(rowData), _, _) = readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await? {
                    rowDatas.push((*position, rowData));
                }
            }

            rowDatas
        };

    Ok(rowDatas)
}

/// 目标是普通表
async fn scanSatisfiedRows(tableFilterExpr: Option<&Expr>,
                           table: &mut Table,
                           select: bool,
                           selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(RowDataPosition, RowData)>> {
    let tableDataFile = table.dataFile.as_mut().unwrap();

    tableDataFile.seek(SeekFrom::Start(0)).await?;

    let satisfiedRows =
        if tableFilterExpr.is_some() || select {
            let mut satisfiedRows = Vec::new();

            let mut dataLenBuffer = [0; 8];

            let mut position: RowDataPosition = 0;

            loop {
                let (rowData, reachEnd, rowDataBinaryLen) = readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await?;
                if reachEnd {
                    break;
                }

                if rowData.is_some() {
                    satisfiedRows.push((position, rowData.unwrap()));
                }

                position += rowDataBinaryLen as RowDataPosition;
            }

            satisfiedRows
        } else {
            vec![(global::TOTAL_DATA_OF_TABLE, HashMap::default())]
        };

    Ok(satisfiedRows)
}

async fn readRow(tableDataFile: &mut File,
                 tableFilterExpr: Option<&Expr>,
                 selectedColumnNames: Option<&Vec<String>>,
                 dataLenBuffer: &mut [u8]) -> Result<(Option<RowData>, ReachEnd, RowContentBinaryLen)> {
    // 相当的坑 有read()和read_buf() 前边看的是len后边看的是capactiy
    // 后边的是不能用的 虽然有Vec::with_capacity() 然而随读取的越多vec本身也会扩容的 后来改为 [0; global::ROW_DATA_LEN_FIELD_LEN]

    // xmin
    let readCount = tableDataFile.read(dataLenBuffer).await?;
    if readCount == 0 {  // reach the end
        return Ok((None, true, 0));
    }

    // xmax
    let readCount = tableDataFile.read(dataLenBuffer).await?;
    if readCount == 0 {  // reach the end
        return Ok((None, true, 0));
    }
    // next valid position
    let readCount = tableDataFile.read(dataLenBuffer).await?;
    if readCount == 0 {  // reach the end
        return Ok((None, true, 0));
    }

    let readCount = tableDataFile.read(&mut dataLenBuffer[0..global::ROW_CONTENT_LEN_FIELD_LEN]).await?;
    if readCount == 0 {  // reach the end
        return Ok((None, true, 0));
    }

    assert_eq!(readCount, global::ROW_CONTENT_LEN_FIELD_LEN);

    let dataLen = u32::from_be_bytes([dataLenBuffer[0], dataLenBuffer[1], dataLenBuffer[2], dataLenBuffer[3]]) as usize;
    let mut dataBuffer = vec![0u8; dataLen];
    let readCount = tableDataFile.read(&mut dataBuffer).await?;
    assert_eq!(readCount, dataLen);
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

    let rowDataBinaryLen = (global::ROW_PREFIX_LEN + dataLen) as RowContentBinaryLen;

    if tableFilterExpr.is_none() {
        return Ok((Some(rowData), false, rowDataBinaryLen));
    }

    if let GraphValue::Boolean(satisfy) = tableFilterExpr.unwrap().calc(Some(&rowData))? {
        if satisfy {
            Ok((Some(rowData), false, rowDataBinaryLen))
        } else {
            Ok((None, false, rowDataBinaryLen))
        }
    } else {
        throw!("table filter should get a boolean")
    }
}

fn getTableRefMutByName(tableName: &str) -> Result<RefMut<String, Table>> {
    let table = global::TABLE_NAME_TABLE.get_mut(tableName);
    if table.is_none() {
        throw!(&format!("table:{} not exist", tableName));
    }
    Ok(table.unwrap())
}

fn generateInsertValuesJson(insert: &Insert, table: &Table) -> Result<Value> {
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

fn writeBytesMut(rowData: &Value) -> Result<BytesMut> {
    let jsonString = serde_json::to_string(rowData)?;

    let jsonStringByte = jsonString.as_bytes();
    let dataLen = jsonStringByte.len();

    assert!(u32::MAX as usize >= jsonStringByte.len());

    let mut bytesMut = BytesMut::with_capacity(global::ROW_PREFIX_LEN + jsonStringByte.len());
    bytesMut.put_u64(0);
    bytesMut.put_u64(0);
    bytesMut.put_u64(0);
    bytesMut.put_u32(dataLen as u32);
    bytesMut.put_slice(jsonStringByte);

    Ok(bytesMut)
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use dashmap::DashMap;
    use serde::{Deserialize, Serialize, Serializer};
    use serde::ser::{SerializeMap, SerializeStruct};
    use serde_json::json;
    use crate::graph_value::GraphValue;
    use crate::{global, meta, parser};
    use crate::command_executor;
    use crate::parser::Command;

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(GraphValue::String("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }

    #[tokio::test]
    pub async fn testSelect() {
        meta::init().await.unwrap();

        // select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car
        let commandVec = parser::parse("select user[id,name](id=1 and 0=0)").unwrap();
        if let Command::Select(ref select) = commandVec[0] {
            command_executor::select(select).await.unwrap();
        }
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
}
