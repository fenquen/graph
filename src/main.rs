#![allow(non_snake_case, unused_imports)]

mod config;
mod command_line;
mod macros;
mod graph_error;
mod global;
mod parser;
mod meta;
mod executor;

use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::string::ToString;
use std::sync::{Arc, RwLock};
use anyhow::Result;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use meta::Table;
use crate::config::CONFIG;
use crate::parser::{Command, Parser};

fn main() -> Result<()> {
    init()?;
    // "create table user (id integer,name string);insert into user values (1,'tom')"
    // "create table car (id integer,color string);insert into car values (1,'red')"
    // "create relation usage (number integer)"
    let commandVec = parser::parse("insert into usage values (1)")?;
    for command in commandVec {
        match command {
            Command::CreateTable(table) => {
                executor::createTable(table, false)?;
            }
            Command::Insert(insertValues) => {
                executor::insertValues(&insertValues)?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn init() -> Result<()> {
    // 生成用来保存表文件和元数据的目录
    fs::create_dir_all::<&Path>(CONFIG.dataDir.as_ref())?;
    fs::create_dir_all::<&Path>(CONFIG.metaDir.as_ref())?;

    // 用来记录表的文件
    let metaDirPath: &Path = CONFIG.metaDir.as_ref();
    let tableRecordFile = OpenOptions::new().write(true).read(true).create(true).append(true).open(metaDirPath.join("table_record"))?;

    unsafe {
        global::TABLE_RECORD_FILE = Some(Arc::new(RwLock::new(tableRecordFile)));
    }

    // 还原
    rebuildTables()?;

    Ok(())
}

fn rebuildTables() -> Result<()> {
    unsafe {
        let mut tableRecordFile = global::TABLE_RECORD_FILE.as_ref().unwrap().read().unwrap();

        let bufReader = BufReader::new(&*tableRecordFile);

        for line in bufReader.lines() {
            let line = line?;
            let table: Table = serde_json::from_str(&line)?;
            executor::createTable(table, true)?;
        }
    }

    Ok(())
}