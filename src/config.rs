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
use crate::raft::GraphRaftNodeId;

lazy_static! {
    pub static ref CONFIG :Config = load();
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub log4RsYamlPath: String,
    pub wsAddr: String,

    pub metaDir: String,
    pub dataDir: String,

    pub sessionMemotySize: usize,
    pub txUndergoingMaxCount: AtomicUsize,

    pub distribute: bool,
    pub raftConfig: RaftConfig,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct RaftConfig {
    pub nodeId: GraphRaftNodeId,

    pub httpAddr: String,
    pub rpcAddr: String,

    pub replicaSize: usize,
    pub regionSize: usize,

    pub dir: String,

    pub multicastHost: String,
    pub multicastPort: u16,
    pub multicastInterfaceHost: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            nodeId: 1,

            httpAddr: "127.0.0.1:9674".to_string(),
            rpcAddr: "127.0.0.1:9677".to_string(),

            replicaSize: 4,
            regionSize: 96 * 1024 * 1024,

            dir: "graph_raft".to_string(),

            multicastHost: "224.0.0.121".to_string(),
            multicastPort: 17071,
            multicastInterfaceHost: "127.0.0.1".to_string(),
        }
    }
}


impl Config {
    pub const DEFAULT_SESSION_MEMORY_SIZE: usize = 2048 * 1024 * 1024;
    pub const MIN_SESSION_MEMORY_SIZE: usize = 1024 * 1024 * 1024;

    pub const DEFAULT_TX_UNDERGOING_MAX_COUNT: usize = 10000;
    pub const MIN_TX_UNDERGOING_MAX_COUNT: usize = 1000;
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log4RsYamlPath: "log4rs.yaml".to_string(),
            wsAddr: "127.0.0.1:9673".to_string(),

            metaDir: "graph_meta".to_string(),
            dataDir: "graph_data".to_string(),

            sessionMemotySize: Config::DEFAULT_SESSION_MEMORY_SIZE,
            txUndergoingMaxCount: AtomicUsize::new(Config::DEFAULT_TX_UNDERGOING_MAX_COUNT),

            distribute: false,
            raftConfig: RaftConfig::default(),
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