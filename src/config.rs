use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process;
use clap::Parser;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::command_line::CommandLine;

lazy_static! {
    pub static ref CONFIG :Config = load();
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Config {
    pub log4RsYamlPath: String,
    pub metaDir: String,
    pub wsAddr: String,
    pub dataDir: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            log4RsYamlPath: "log4rs.yaml".to_string(),
            metaDir: "meta".to_string(),
            wsAddr: "127.0.0.1:9673".to_string(),
            dataDir: "data".to_string(),
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
            log::info!("配置文件 {} 不存在 {}",configFilePath, e);
            process::exit(1);
        }
    };

    let mut configJsonFileContent = String::new();
    configJsonFile.read_to_string(&mut configJsonFileContent).unwrap();

    let config: Config = match serde_json::from_str(&configJsonFileContent) {
        Ok(config) => config,
        Err(e) => {
            log::info!("读取配置文件 {} 错误 {}",configFilePath, e);
            process::exit(1);
        }
    };

    config
}