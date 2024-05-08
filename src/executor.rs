use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::ptr::read;
use crate::config::CONFIG;
use crate::{executor, global, prefix_plus_plus, throw};
use crate::meta::{Column, Table, TableType};
use crate::parser::{InsertValues, Link, Select};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use bytes::{BufMut, BytesMut};
use crate::expr::Expr;
use crate::graph_value::{GraphValue, PointDesc};

type RowData = HashMap<String, GraphValue>;

pub async fn createTable(mut table: Table, restore: bool) -> Result<()> {
    if global::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
        throw!(&format!("table/relation: {} already exist",table.name))
    }

    // 表对应的data 文件
    let tableDataFilePath = (CONFIG.dataDir.as_ref() as &Path).join(&table.name);

    if restore == false {
        if tableDataFilePath.exists() {
            throw!(&format!("data file of table:{} has already exist", table.name));
        }

        File::create(tableDataFilePath.as_path()).await?;

        // table_record 文件
        let jsonString = serde_json::to_string(&table)?;
        unsafe {
            let mut tableRecordFile = global::TABLE_RECORD_FILE.as_ref().unwrap().write().await;
            tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r', b'\n']].concat().as_ref()).await?
        };
    }

    let tableDataFile = OpenOptions::new().write(true).read(true).create(true).append(true).open(tableDataFilePath.as_path()).await?;
    table.dataFile = Some(tableDataFile);

    // map
    global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

    Ok(())
}

pub async fn insertValues(insertValues: &InsertValues) -> Result<()> {
    // 对应的表是不是exist
    let mut table = getTableRefMutByName(&insertValues.tableName)?;

    // 不能对relation使用insert into
    if let TableType::Relation = table.type0 {
        throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insertValues.tableName));
    }

    let rowData = generateInsertValuesJson(&insertValues, &*table)?;
    let bytesMut = writeBytesMut(&rowData)?;
    table.dataFile.as_mut().unwrap().write_all(bytesMut.as_ref()).await?;

    Ok(())
}

/// 它本质是向relation对应的data file写入
/// 两个元素之间的relation只看种类不看里边的属性的
pub async fn link(link: &Link) -> Result<()> {
    // 得到3个表的对象
    let mut srcTable = getTableRefMutByName(link.srcTableName.as_str())?;
    let mut destTable = getTableRefMutByName(link.destTableName.as_str())?;

    // 对src table和dest table调用expr筛选
    let srcTableSatisfiedRowNums = scanSatisfiedRows(link.srcTableFilterExpr.as_ref(), srcTable.value_mut(), false, None).await?;
    let destTableSatisfiedRowNums = scanSatisfiedRows(link.destTableFilterExpr.as_ref(), destTable.value_mut(), false, None).await?;

    // 用insetValues套路
    {
        let insertValues = InsertValues {
            tableName: link.destTableName.clone(),
            useExplicitColumnNames: true,
            columnNames: link.relationColumnNames.clone(),
            columnExprs: link.relationColumnExprs.clone(),
        };
        let mut relationTable = getTableRefMutByName(link.relationName.as_str())?;
        let mut rowData = generateInsertValuesJson(&insertValues, &*relationTable)?;

        rowData["src"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:srcTable.name.clone(),
            positions:srcTableSatisfiedRowNums.iter().map(|tuple|tuple.0).collect(),
        }));
        rowData["dest"] = json!(GraphValue::PointDesc(PointDesc {
            tableName:destTable.name.clone(),
            positions:destTableSatisfiedRowNums.iter().map(|tuple|tuple.0).collect(),
        }));

        let bytesMut = writeBytesMut(&rowData)?;

        let relationDataFile = relationTable.dataFile.as_mut().unwrap();
        // bytesMut.as_ref() 也可以使用 &bytesMut[..]
        relationDataFile.write_all(bytesMut.as_ref()).await?;
    }

    Ok(())
}

/// 如果不是含有relation的select 便是普通的select
pub async fn select(select: &[Select]) -> Result<()> {
    for select in select {
        // 先要明确是不是含有relation
        match select.relationName {
            Some(ref relationName) => {
                // 为什么要使用{} 不然的话有概率死锁 https://savannahar68.medium.com/deadlock-issues-in-rusts-dashmap-a-practical-case-study-ad08f10c2849
                let relationDatas: Vec<HashMap<String, GraphValue>> = {
                    let mut relation = getTableRefMutByName(relationName)?;
                    let relationDatas = scanSatisfiedRows(select.relationFliterExpr.as_ref(), relation.value_mut(), true, select.relationColumnNames.as_ref()).await?;
                    relationDatas.into_iter().map(|tuple| tuple.1).collect()
                };

                #[derive(Deserialize, Debug)]
                struct SelectResult {
                    relationData: RowData,
                    srcRowDatas: Vec<RowData>,
                    destRowDatas: Vec<RowData>,
                }

                let mut selectResultVec = Vec::with_capacity(relationDatas.len());

                for relationData in relationDatas {
                    let srcPointDesc = relationData.get(PointDesc::SRC).unwrap().asPointDesc()?;
                    // relation的src表的name不符合
                    if srcPointDesc.tableName != select.srcName {
                        continue;
                    }

                    // relation的dest表的name不符合
                    let destPointDesc = relationData.get(PointDesc::DEST).unwrap().asPointDesc()?;
                    if destPointDesc.tableName != (*select.destName.as_ref().unwrap()) {
                        continue;
                    }

                    if srcPointDesc.positions.is_empty() || destPointDesc.positions.is_empty() {
                        continue;
                    }

                    let srcRowDatas = {
                        let mut srcTable = getTableRefMutByName(select.srcName.as_str())?;
                        getRowsByPositions(&srcPointDesc.positions, &mut srcTable, select.srcFilterExpr.as_ref(), select.srcColumnNames.as_ref()).await?
                    };

                    let destRowDatas = {
                        let mut destTable = getTableRefMutByName(select.destName.as_ref().unwrap())?;
                        getRowsByPositions(&destPointDesc.positions, &mut destTable, select.destFilterExpr.as_ref(), select.destColumnNames.as_ref()).await?
                    };

                    selectResultVec.push(
                        SelectResult {
                            relationData,
                            srcRowDatas,
                            destRowDatas,
                        });
                }

                println!("{:?}\n", selectResultVec)
            }
            None => {
                let mut srcTable = getTableRefMutByName(select.srcName.as_str())?;
                let rows = scanSatisfiedRows(select.srcFilterExpr.as_ref(), srcTable.value_mut(), true, select.srcColumnNames.as_ref()).await?;
                let rows: Vec<HashMap<String, GraphValue>> = rows.into_iter().map(|tuple| tuple.1).collect();
                println!("{:?}", rows);
            }
        }
    }

    Ok(())
}

async fn getRowsByPositions(positions: &[u64],
                            table: &mut Table,
                            tableFilterExpr: Option<&Expr>,
                            selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<RowData>> {
    let tableDataFile = table.dataFile.as_mut().unwrap();

    let mut dataLenBuffer = [0; global::ROW_DATA_LEN_FIELD_LEN];

    let mut rowDatas = vec![];

    for position in positions {
        tableDataFile.seek(SeekFrom::Start(*position)).await?;
        if let (Some(rowData), _) = readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await? {
            rowDatas.push(rowData);
        }
    }

    Ok(rowDatas)
}

async fn scanSatisfiedRows(tableFilterExpr: Option<&Expr>,
                           table: &mut Table,
                           select: bool,
                           selectedColumnNames: Option<&Vec<String>>) -> Result<Vec<(u64, RowData)>> {
    let tableDataFile = table.dataFile.as_mut().unwrap();

    tableDataFile.seek(SeekFrom::Start(0)).await?;

    let satisfiedRows =
        if tableFilterExpr.is_some() || select {
            let mut satisfiedRows = Vec::new();

            let mut dataLenBuffer = [0; global::ROW_DATA_LEN_FIELD_LEN];

            loop {
                let position = tableDataFile.seek(SeekFrom::Current(0)).await?;

                let (rowData, reachEnd) = readRow(tableDataFile, tableFilterExpr, selectedColumnNames, &mut dataLenBuffer).await?;
                if reachEnd {
                    break;
                }

                if rowData.is_some() {
                    satisfiedRows.push((position, rowData.unwrap()));
                }
            }

            satisfiedRows
        } else {
            vec![(u64::MAX, HashMap::default())]
        };

    Ok(satisfiedRows)
}

async fn readRow(tableDataFile: &mut File,
                 tableFilterExpr: Option<&Expr>,
                 selectedColumnNames: Option<&Vec<String>>,
                 dataLenBuffer: &mut [u8]) -> Result<(Option<RowData>, bool)> {
    // 相当的坑 有read()和read_buf() 前边看的是len后边看的是capactiy
    // 后边的是不能用的 虽然有Vec::with_capacity() 然而随读取的越多vec本身也会扩容的 后来改为 [0; global::ROW_DATA_LEN_FIELD_LEN]
    let readCount = tableDataFile.read(dataLenBuffer).await?;
    if readCount == 0 {  // reach the end
        return Ok((None, true));
    }

    assert_eq!(readCount, global::ROW_DATA_LEN_FIELD_LEN);

    let dataLen = u32::from_be_bytes([dataLenBuffer[0], dataLenBuffer[1], dataLenBuffer[2], dataLenBuffer[3]]) as usize;
    let mut dataBuffer = vec![0u8; dataLen];
    let readCount = tableDataFile.read(&mut dataBuffer).await?;
    assert_eq!(readCount, dataLen);
    let jsonString = String::from_utf8(dataBuffer)?;

    let mut rowData: HashMap<String, GraphValue> = serde_json::from_str(&jsonString)?;

    let rowData = if selectedColumnNames.is_some() {
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
        return Ok((Some(rowData), false));
    }

    if let GraphValue::Boolean(satisfy) = tableFilterExpr.unwrap().calc(Some(&rowData))? {
        if satisfy {
            Ok((Some(rowData), false))
        } else {
            Ok((None, false))
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

fn generateInsertValuesJson(insertValues: &InsertValues, table: &Table) -> Result<Value> {
    let columns = {
        let mut columns = Vec::new();

        // 要是未显式说明column的话还需要读取table的column
        if insertValues.useExplicitColumnNames == false {
            for column in &table.columns {
                columns.push(column);
            }
        } else { // 如果显式说明columnName的话需要确保都是有的
            for columnName in &insertValues.columnNames {
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
    if columns.len() != insertValues.columnExprs.len() {
        throw!("column count does not match value count");
    }

    let mut rowData = json!({});

    for column_columnValue in columns.iter().zip(insertValues.columnExprs.iter()) {
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

    let mut bytesMut = BytesMut::with_capacity(global::ROW_DATA_LEN_FIELD_LEN + jsonStringByte.len());
    bytesMut.put_u32(dataLen as u32);
    bytesMut.put_slice(jsonStringByte);

    Ok(bytesMut)
}


#[cfg(test)]
mod test {
    use dashmap::DashMap;
    use serde_json::json;
    use crate::graph_value::GraphValue;
    use crate::{meta, parser};
    use crate::executor;
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
            executor::select(select).await.unwrap();
        }
    }

    #[test]
    pub fn dash() {
        let map = DashMap::with_capacity(2);
        map.insert("a".to_string(), "a");
        map.insert("r".to_string(), "r");

        map.get_mut("a");
        map.get_mut("b");
    }
}
