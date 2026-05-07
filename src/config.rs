use std::fs::File;
use std::io::Read;
use std::net::Ipv4Addr;
use std::path::Path;
use std::process;
use std::sync::atomic::AtomicUsize;
use clap::Parser;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::command_line::CommandLine;

lazy_static! {
    pub static ref CONFIG :Config = load();
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub log4RsYamlPath: String,
    pub wsAddr: String,
    pub dataDir: String,
    pub flyingTxMaxCount: AtomicUsize,
    pub tempFileDir: String,
}

impl Config {
    /// 2GB
    pub const SESSION_MEMORY_SIZE_DEFAULT: usize = 2048 * 1024 * 1024;
    /// 1GB
    pub const SESSION_MEMORY_SIZE_MIN: usize = 1024 * 1024 * 1024;

    pub const FLYING_TX_MAX_COUNT_DEFAULT: usize = 10000;
    pub const FLYING_TX_MAX_COUNT_MIN: usize = 1000;

    // 2MB
    pub const DEFAULT_WORKING_MEMORY_SIZE: usize = 2 * 1024 * 1024;
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log4RsYamlPath: "log4rs.yaml".to_string(),
            wsAddr: "127.0.0.1:9673".to_string(),
            dataDir: "graph_data".to_string(),
            flyingTxMaxCount: AtomicUsize::new(Self::FLYING_TX_MAX_COUNT_DEFAULT),
            tempFileDir: "temp".to_string(),
        }
    }
}

pub fn load() -> Config {
    let commandLine = CommandLine::parse();

    if commandLine.configFilePath.is_none() {
        return Config::default();
    }

    let configFilePath = commandLine.configFilePath.as_ref().unwrap();

    let mut configJsonFile =
        match File::open(configFilePath) {
            Ok(f) => f,
            Err(e) => {
                log::info!("config file:{} not exist, {}", configFilePath, e);
                process::exit(1);
            }
        };

    let configJsonFileContent = {
        let mut configJsonFileContent = String::new();
        configJsonFile.read_to_string(&mut configJsonFileContent).unwrap();

        configJsonFileContent
    };

    match serde_json::from_str(&configJsonFileContent) {
        Ok(config) => config,
        Err(e) => {
            log::info!("reading config file:{} error, {}", configFilePath, e);
            process::exit(1);
        }
    }
}