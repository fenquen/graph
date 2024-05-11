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
mod session;

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
    let mut sqls = bufReader.lines();
    while let Some(sql) = sqls.next_line().await? {
        if sql.starts_with("--") {
            continue;
        }

        let commandVec = parser::parse(sql.as_str())?;
        executor::execute(commandVec).await?;
    }

    Ok(())
}