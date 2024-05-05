#![allow(non_snake_case, unused_imports)]

mod config;
mod command_line;
mod macros;
mod graph_error;
mod global;
mod parser;
mod meta;
mod executor;
mod a;
mod expr;
mod graph_value;

use std::path::Path;
use std::string::ToString;
use std::sync::{Arc};
use anyhow::Result;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;
use meta::Table;
use crate::config::CONFIG;
use crate::parser::{Command, Parser};

#[tokio::main]
pub async fn main() -> Result<()> {
    init().await?;

    // "create table user (id integer,name string);insert into user values (1,'tom')"
    // "create table car (id integer,color string);insert into car values (34,'red')"
    // "create relation usage (number integer)"
    // "link user(id =1) to car(color='red') by usage(number = 12)"
    // "link user(id =1) to car(id =37) by usage(number = 17)"
    let commandVec = parser::parse("link user(id =1) to car(color='red') by usage(number = 12)")?;
    for command in commandVec {
        match command {
            Command::CreateTable(table) => executor::createTable(table, false).await?,
            Command::Insert(insertValues) => executor::insertValues(insertValues).await?,
            Command::Link(link) => executor::link(link).await?,
            _ => {}
        }
    }

    Ok(())
}

async fn init() -> Result<()> {
    // 生成用来保存表文件和元数据的目录
    fs::create_dir_all(CONFIG.dataDir.as_str()).await?;
    fs::create_dir_all(CONFIG.metaDir.as_str()).await?;

    // 用来记录表的文件
    let metaDirPath: &Path = CONFIG.metaDir.as_ref();
    let tableRecordFile = OpenOptions::new().write(true).read(true).create(true).append(true).open(metaDirPath.join("table_record")).await?;

    unsafe {
        global::TABLE_RECORD_FILE = Some(Arc::new(RwLock::new(tableRecordFile)));
    }

    // 还原
    rebuildTables().await?;

    Ok(())
}

async fn rebuildTables() -> Result<()> {
    unsafe {
        let mut tableRecordFile = global::TABLE_RECORD_FILE.as_ref().unwrap().write().await;

        let bufReader = BufReader::new(&mut *tableRecordFile);
        let mut lines = bufReader.lines();
        while let Some(line) = lines.next_line().await? {
            let table: Table = serde_json::from_str(&line)?;
            executor::createTable(table, true).await?;
        }
    }

    Ok(())
}