use std::path::Path;
use std::fs::File;
use std::io::Write;
use crate::config::CONFIG;
use crate::{global, throw};
use crate::meta::{Column, Table};
use crate::parser::InsertValues;
use anyhow::Result;
use serde_json::{json, Value};


pub fn createTable(table: Table, replay: bool) -> Result<()> {
    let dataDirPath: &Path = CONFIG.dataDir.as_ref();

    let tableDataFilePath = dataDirPath.join(&table.name);
    if tableDataFilePath.exists() {
        throw!(&format!("table {} has already exist", table.name));
    }

    File::create(tableDataFilePath)?;

    let jsonString = serde_json::to_string(&table)?;

    if replay == false {
        unsafe {
            let mut tableRecordFile = global::TABLE_RECORD_FILE.as_ref().unwrap().write().unwrap();
            tableRecordFile.write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref())
        }?;
    }

    global::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

    Ok(())
}

pub fn insertValues(insertValues: &InsertValues) -> Result<()> {
    // 对应的表是不是exist
    let option = global::TABLE_NAME_TABLE.get(&insertValues.tableName);
    if option.is_none() {
        throw!(&format!("table {} not exist", insertValues.tableName));
    }
    let table = &*option.unwrap();

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

    let mut rowData = json!({});
    rowData["1a"] = json!("a");

    for column_columnValue in columns.iter().zip(insertValues.columnValues.iter()) {
        let column = column_columnValue.0;
        let columnValue = column_columnValue.1;

        // columnType和value也要对上
        if column.type0.compatible(columnValue) == false {
            throw!(&format!("column:{},type:{} is not compatible with value:{}", column.name, column.type0, columnValue));
        }

        rowData[column.name.as_str()] = json!(columnValue);
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use crate::meta::ColumnValue;

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(ColumnValue::STRING("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }
}
