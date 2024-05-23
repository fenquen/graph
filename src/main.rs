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
mod ws;

use std::string::ToString;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use crate::config::CONFIG;
use crate::parser::Command;
use crate::session::Session;

#[tokio::main]
pub async fn main() -> Result<()> {
    log4rs::init_file(config::CONFIG.log4RsYamlPath.as_str(), Default::default())?;

    meta::init()?;

    ws::init().await?;

    Ok(())
}
