#![allow(non_snake_case, unused_imports)]

extern crate core;

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

use std::string::ToString;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use crate::parser::Command;

#[tokio::main]
pub async fn main() -> Result<()> {
    meta::init().await?;

    let tableRecordFile = OpenOptions::new().read(true).open("sql.txt").await?;
    let bufReader = BufReader::new(tableRecordFile);
    let mut lines = bufReader.lines();
    while let Some(sql) = lines.next_line().await? {
        if sql.starts_with("--") {
            continue;
        }

        let commandVec = parser::parse(sql.as_str())?;
        for command in commandVec {
            match command {
                Command::CreateTable(table) => executor::createTable(table, false).await?,
                Command::Insert(ref insertValues) => executor::insert(insertValues).await?,
                Command::Select(ref select) => executor::select(select).await?,
                Command::Link(ref link) => executor::link(link).await?,
                _ => {}
            }
        }
    }

    Ok(())
}