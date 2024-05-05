use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::path::Path;
use crate::config::CONFIG;
use crate::{executor, global, prefix_plus_plus, throw};
use crate::meta::{Column, GraphValue, Table, TableType};
use crate::parser::{InsertValues, Link};
use anyhow::Result;
use dashmap::mapref::one::{Ref, RefMut};
use serde_json::{json, Map, Value};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use crate::expr::Expr;

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

pub async fn insertValues(insertValues: InsertValues) -> Result<()> {
    // 对应的表是不是exist
    let mut table = getTableRefMutByName(&insertValues.tableName)?;

    // 不能对relation使用insert into
    if let TableType::RELATION = table.type0 {
        throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insertValues.tableName));
    }

    let jsonString = serde_json::to_string(&generateInsertValuesJson(&insertValues, &*table)?)?;
    table.dataFile.as_mut().unwrap().write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref()).await?;

    Ok(())
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

pub async fn link(link: Link) -> Result<()> {
    // 得到3个表的对象
    let mut srcTable = getTableRefMutByName(link.srcTableName.as_str())?;
    let mut destTable = getTableRefMutByName(link.destTableName.as_str())?;

    async fn getSatisfiedRowNumVec(tableFilterExpr: Option<&Expr>, table: &mut Table) -> Result<Vec<usize>> {
        Ok(if tableFilterExpr.is_some() {
            let srcTableFilterExpr = tableFilterExpr.unwrap();
            let mut rowNum: usize = 0;
            let mut satisfiedRowNumVec = Vec::new();

            let srcTableDataFile = table.dataFile.as_mut().unwrap();
            let bufReader = BufReader::new(srcTableDataFile);
            let mut lines = bufReader.lines();
            while let Some(line) = lines.next_line().await? {
                prefix_plus_plus!(rowNum);

                let rowData: HashMap<String, GraphValue> = serde_json::from_str(&line)?;
                if let GraphValue::Boolean(satisfy) = srcTableFilterExpr.calc(Some(&rowData))? {
                    if satisfy {
                        satisfiedRowNumVec.push(rowNum);
                    }
                } else {
                    throw!("table filter should get a boolean")
                }
            }

            satisfiedRowNumVec
        } else {
            vec![0]
        })
    }

    // 对src table和dest table调用expr筛选
    let srcTableSatisfiedRowNums = getSatisfiedRowNumVec(link.srcTableFilterExpr.as_ref(), srcTable.value_mut()).await?;
    let destTableSatisfiedRowNums = getSatisfiedRowNumVec(link.destTableFilterExpr.as_ref(), destTable.value_mut()).await?;

    // 用insetValues套路
    let insertValues = InsertValues {
        tableName: link.destTableName.clone(),
        useExplicitColumnNames: true,
        columnNames: link.relationColumnNames.clone(),
        columnExprs: link.relationColumnExprs.clone(),
    };
    let mut relationTable = getTableRefMutByName(link.relationName.as_str())?;
    let mut rowData = generateInsertValuesJson(&insertValues, &*relationTable)?;
    rowData["srcRowNums"] = json!(srcTableSatisfiedRowNums);
    rowData["destRowNums"] = json!(destTableSatisfiedRowNums);

    let jsonString = serde_json::to_string(&rowData)?;

    relationTable.dataFile.as_mut().unwrap().write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref()).await?;
    Ok(())
}


fn getTableRefMutByName(tableName: &str) -> Result<RefMut<String, Table>> {
    let table = global::TABLE_NAME_TABLE.get_mut(tableName);
    if table.is_none() {
        throw!(&format!("table:{} not exist", tableName));
    }
    Ok(table.unwrap())
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use crate::meta::GraphValue;

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(GraphValue::String("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }
}
