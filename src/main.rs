#![allow(non_snake_case, unused_imports)]

mod config;
mod command_line;
mod macros;
mod graph_error;
mod global;
mod parser;
mod meta;
mod command_executor;
mod a;
mod expr;
mod graph_value;
mod session;
mod codec;
mod utils;

use std::string::ToString;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use crate::parser::Command;
use crate::session::Session;

#[tokio::main]
pub async fn main() -> Result<()> {
    meta::init()?;

    let tableRecordFile = OpenOptions::new().read(true).open("sql.txt").await?;
    let bufReader = BufReader::new(tableRecordFile);
    let mut sqls = bufReader.lines();

    let mut session = Session::new();

    while let Some(sql) = sqls.next_line().await? {
        if sql.starts_with("--") {
            continue;
        }

        session.executeSql(sql.as_str()).await?;
    }

    Ok(())
}