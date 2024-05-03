use std::cell::UnsafeCell;
use std::path::Path;
use crate::config::CONFIG;
use crate::{global, throw};
use crate::meta::{Column, Table, TableType};
use crate::parser::{InsertValues, Link};
use anyhow::Result;
use dashmap::mapref::one::Ref;
use serde_json::{json, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub async fn createTable(mut table: Table, restore: bool) -> Result<()> {
    let dataDirPath: &Path = CONFIG.dataDir.as_ref();

    // 表对应的data 文件
    let tableDataFilePath = dataDirPath.join(&table.name);
    let tableDataFileExist = tableDataFilePath.exists();
    if restore {
        if tableDataFileExist == false {
            throw!(&format!("data file of table:{} not exist", table.name));
        }
    } else {
        if tableDataFileExist {
            throw!(&format!("data file of table:{} has already exist", table.name));
        }

        File::create(tableDataFilePath).await?;
    }

    // table_record 文件
    if restore == false {
        let jsonString = serde_json::to_string(&table)?;
        unsafe {
            let mut tableRecordFile = global::TABLE_RECORD_FILE.as_ref().unwrap().write().await;
            tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref()).await?
        };
    }

    let dataDirPath: &Path = CONFIG.dataDir.as_ref();
    let tableDataFile = OpenOptions::new().write(true).read(true).create(true).open(dataDirPath.join(table.name.as_str())).await?;
    table.dataFile = Some(tableDataFile);

    // map
    global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

    Ok(())
}

pub async fn insertValues(insertValues: &InsertValues) -> Result<()> {
    // 对应的表是不是exist
    let option = global::TABLE_NAME_TABLE.get_mut(&insertValues.tableName);
    if option.is_none() {
        throw!(&format!("table {} not exist", insertValues.tableName));
    }
    let table = &mut *option.unwrap();

    // 不能对relation使用insert into
    if let TableType::RELATION = table.type0 {
        throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insertValues.tableName));
    }

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
    if columns.len() != insertValues.columnValues.len() {
        throw!("column count does not match value count");
    }

    let mut rowData = json!({});

    for column_columnValue in columns.iter().zip(insertValues.columnValues.iter()) {
        let column = column_columnValue.0;
        let columnValue = column_columnValue.1;

        // columnType和value也要对上
        if column.type0.compatible(columnValue) == false {
            throw!(&format!("column:{},type:{} is not compatible with value:{}", column.name, column.type0, columnValue));
        }

        rowData[column.name.as_str()] = json!(columnValue);
    }

    let jsonString = serde_json::to_string(&rowData)?;
    table.dataFile.as_mut().unwrap().write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref()).await?;

    Ok(())
}

pub async fn link(link: &Link) -> Result<()> {
    // 得到3个表的对象
    let srcTable = getTableRefByName(link.srcTableName.as_str())?;
    let destTable = getTableRefByName(link.destTableName.as_str())?;
    let relation = getTableRefByName(link.relationName.as_str())?;

    // 对src table和dest table调用expr筛选
    let srcTableFilterExpr = link.srcTableFilterExpr.as_ref().unwrap_or_default();

    Ok(())
}

fn getTableRefByName(tableName: &str) -> Result<Ref<String,Table>> {
    let table = global::TABLE_NAME_TABLE.get(tableName);
    if table.is_none() {
        throw!(&format!("table:{} not exist", tableName));
    }
    Ok(table.unwrap())
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use crate::meta::Value;

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(Value::STRING("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }
}
