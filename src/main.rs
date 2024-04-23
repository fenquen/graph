#![allow(non_snake_case, unused_imports)]

mod config;
mod command_line;
mod macros;
mod graph_error;
mod global;
mod parser;

use std::fs;
use once_cell::sync::Lazy;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::string::ToString;
use std::sync::{Arc, Mutex, RwLock};
use anyhow::Result;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::ColumnType::{DOUBLE, LONG, STRING, UNKNOWN};
use crate::config::CONFIG;
use crate::parser::Command;

lazy_static! {
    pub static ref SAMPLE_TABLE_DESC: Table = Table {
        name: "test".to_string(),
        columns: vec![Column {
            name: "column1".to_string(),
            type0: ColumnType::STRING,
        }],
        type0: TableType::DATA,
    };
}

fn main() -> Result<()> {
    init()?;

    Ok(())
}

fn init() -> Result<()> {
    // 生成用来保存表文件和元数据的目录
    fs::create_dir_all::<&Path>(CONFIG.dataDir.as_ref())?;
    fs::create_dir_all::<&Path>(CONFIG.metaDir.as_ref())?;

    // 用来记录表的文件
    let metaDirPath: &Path = CONFIG.metaDir.as_ref();
    let tableRecordFile = OpenOptions::new().write(true).read(true).create(true).open(metaDirPath.join("table_record"))?;

    // 还原
    rebuildTables(&tableRecordFile)?;

    unsafe {
        global::TABLE_RECORD_FILE = Some(Arc::new(RwLock::new(tableRecordFile)));
    }

    Ok(())
}

fn rebuildTables(tableRecordFile: &File) -> Result<()> {
    let bufReader = BufReader::new(tableRecordFile);

    for line in bufReader.lines() {
        let line = line?;
        let table: Table = serde_json::from_str(&line)?;
        global::TABLE_NAME_TABLE.insert(table.name.clone(), table);
    }

    Ok(())
}

pub fn createTable(table: &Table) -> Result<()> {
    let baseDirPath: &Path = CONFIG.dataDir.as_ref();

    let tablePath = baseDirPath.join(&table.name);
    if tablePath.exists() {
        throw!(&format!("table {} has already exist", table.name))
    }


    File::create(tablePath)?;

    let jsonString = serde_json::to_string(table)?;

    unsafe {
        let mut tableDescFile = global::TABLE_RECORD_FILE.as_ref().unwrap().write().unwrap();
        tableDescFile.write_all([jsonString.as_bytes(), &[b'\r'], &[b'\n']].concat().as_ref())
    }?;


    Ok(())
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub type0: TableType,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub enum TableType {
    DATA,
    RELATION,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Column {
    pub name: String,
    pub type0: ColumnType,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub enum ColumnType {
    STRING,
    LONG,
    DOUBLE,
    UNKNOWN,
}

impl From<&str> for ColumnType {
    fn from(value: &str) -> Self {
        match value {
            "STRING" => STRING,
            "LONG" => LONG,
            "DOUBLE" => DOUBLE,
            _ => UNKNOWN
        }
    }
}