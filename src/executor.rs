use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::ptr::read;
use crate::config::CONFIG;
use crate::{executor, global, prefix_plus_plus, throw};
use crate::meta::{Column, Table, TableType};
use crate::parser::{InsertValues, Link};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use bytes::{BufMut, BytesMut};
use crate::expr::Expr;
use crate::graph_value::GraphValue;

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
            tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref()).await?
        };
    }

    let tableDataFile = OpenOptions::new().write(true).read(true).create(true).append(true).open(tableDataFilePath.as_path()).await?;
    table.dataFile = Some(tableDataFile);

    // map
    global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

    Ok(())
}

pub async fn insertValues(insertValues: InsertValues) -> Result<()> {
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
pub async fn link(link: Link) -> Result<()> {
    // 得到3个表的对象
    let mut srcTable = getTableRefMutByName(link.srcTableName.as_str())?;
    let mut destTable = getTableRefMutByName(link.destTableName.as_str())?;

    async fn getSatisfiedRowNumVec(tableFilterExpr: Option<&Expr>, table: &mut Table) -> Result<Vec<u64>> {
        let tableDataFile = table.dataFile.as_mut().unwrap();

        tableDataFile.seek(SeekFrom::Start(0)).await?;

        let satisfiedRowNumVec =
            if tableFilterExpr.is_some() {
                let mut satisfiedRowNumVec = Vec::new();
                let mut dataLenBuffer = vec![0; global::ROW_DATA_LEN_FIELD_LEN];

                loop {
                    let position = tableDataFile.seek(SeekFrom::Current(0)).await?;

                    // 相当的坑 有read()和read_buf() 前边看的是len后边看的是capactiy
                    // 后边的是不能用的 虽然有Vec::with_capacity() 然而随读取的越多vec本身也会扩容的
                    let readCount = tableDataFile.read(&mut dataLenBuffer).await?;
                    if readCount == 0 {  // reach the end
                        break;
                    } else {
                        assert_eq!(readCount, global::ROW_DATA_LEN_FIELD_LEN);
                    }

                    let dataLen = u32::from_be_bytes([dataLenBuffer[0], dataLenBuffer[1], dataLenBuffer[2], dataLenBuffer[3]]) as usize;
                    let mut dataBuffer = vec![0u8; dataLen];
                    let readCount = tableDataFile.read(&mut dataBuffer).await?;
                    assert_eq!(readCount, dataLen);
                    let jsonString = String::from_utf8(dataBuffer)?;

                    let rowData: HashMap<String, GraphValue> = serde_json::from_str(&jsonString)?;
                    if let GraphValue::Boolean(satisfy) = tableFilterExpr.unwrap().calc(Some(&rowData))? {
                        if satisfy {
                            satisfiedRowNumVec.push(position);
                        }
                    } else {
                        throw!("table filter should get a boolean")
                    }
                }

                satisfiedRowNumVec
            } else {
                vec![0]
            };

        Ok(satisfiedRowNumVec)
    }

    // 对src table和dest table调用expr筛选
    let srcTableSatisfiedRowNums = getSatisfiedRowNumVec(link.srcTableFilterExpr.as_ref(), srcTable.value_mut()).await?;
    let destTableSatisfiedRowNums = getSatisfiedRowNumVec(link.destTableFilterExpr.as_ref(), destTable.value_mut()).await?;

    // 用insetValues套路
    {
        #[derive(Serialize, Deserialize)]
        struct Node {
            tableName: String,
            positions: Vec<u64>,
        }

        let insertValues = InsertValues {
            tableName: link.destTableName.clone(),
            useExplicitColumnNames: true,
            columnNames: link.relationColumnNames.clone(),
            columnExprs: link.relationColumnExprs.clone(),
        };
        let mut relationTable = getTableRefMutByName(link.relationName.as_str())?;
        let mut rowData = generateInsertValuesJson(&insertValues, &*relationTable)?;

        rowData["src"] = json!(Node {
            tableName:srcTable.name.clone(),
            positions:srcTableSatisfiedRowNums,
        });
        rowData["dest"] = json!(Node {
            tableName:destTable.name.clone(),
            positions:destTableSatisfiedRowNums,
        });

        let bytesMut = writeBytesMut(&rowData)?;

        let relationDataFile = relationTable.dataFile.as_mut().unwrap();
        // bytesMut.as_ref() 也可以使用 &bytesMut[..]
        relationDataFile.write_all(bytesMut.as_ref()).await?;
    }

    Ok(())
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
    use serde_json::json;
    use crate::graph_value::GraphValue;

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(GraphValue::String("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }
}
