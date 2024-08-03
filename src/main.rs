#![allow(non_snake_case)]
#![allow(unused_imports)]
#![feature(trait_alias)]
#![feature(allocator_api)]
#![feature(iter_collect_into)]
#![allow(unused)]

mod config;
mod command_line;
mod macros;
mod graph_error;
mod global;
mod meta;
mod a;
mod expr;
mod graph_value;
mod session;
mod codec;
mod utils;
mod ws;
mod types;
mod executor;
mod parser;
mod raft;

use std::string::ToString;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
#[tokio::main]
pub async fn main() -> Result<()> {
    log4rs::init_file(config::CONFIG.log4RsYamlPath.as_str(), Default::default())?;

    if config::CONFIG.distribute {
        let (send, receive) = raft::multicast::init()?;
        send.join();
        receive.join();
    } else {
        meta::init()?;
        ws::init().await?;
    }

    Ok(())
}
