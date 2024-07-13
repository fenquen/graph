use std::fs::File;
use std::io::Read;
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
    pub metaDir: String,
    pub wsAddr: String,
    pub dataDir: String,
    pub sessionMemotySize: usize,
    pub txUndergoingMaxCount: AtomicUsize,
}

impl Config {
    pub const DEFAULT_SESSION_MEMORY_SIZE: usize = 2048 * 1024 * 1024;
    pub const MIN_SESSION_MEMORY_SIZE: usize = 1024 * 1024 * 1024;

    pub const DEFAULT_TX_UNDERGOING_MAX_COUNT: usize = 10000;
    pub const MIN_TX_UNDERGOING_MAX_COUNT: usize = 1000;
}

impl Default for Config {
    fn default() -> Self {
        Config {
            log4RsYamlPath: "log4rs.yaml".to_string(),
            metaDir: "meta".to_string(),
            wsAddr: "127.0.0.1:9673".to_string(),
            dataDir: "data".to_string(),
            sessionMemotySize: Config::DEFAULT_SESSION_MEMORY_SIZE,
            txUndergoingMaxCount: AtomicUsize::new(Config::DEFAULT_TX_UNDERGOING_MAX_COUNT),
        }
    }
}

pub fn load() -> Config {
    let commandLine = CommandLine::parse();

    if commandLine.configFilePath.is_none() {
        return Config::default();
    }

    let configFilePath = commandLine.configFilePath.as_ref().unwrap();

    let mut configJsonFile = match File::open(configFilePath) {
        Ok(f) => f,
        Err(e) => {
            log::info!("config file:{} not exist, {}", configFilePath, e);
            process::exit(1);
        }
    };

    let mut configJsonFileContent = String::new();
    configJsonFile.read_to_string(&mut configJsonFileContent).unwrap();

    let config: Config = match serde_json::from_str(&configJsonFileContent) {
        Ok(config) => config,
        Err(e) => {
            log::info!("reading config file:{} error, {}", configFilePath, e);
            process::exit(1);
        }
    };

    config
}