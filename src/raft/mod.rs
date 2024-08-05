use std::sync::{Arc, RwLock};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::{config, throwFormat};
use crate::types::{Byte, ColumnFamily};
use std::io::Cursor;
use anyhow::Result;
use rocksdb::{DB, Options};
use crate::utils::TrickyContainer;

pub mod multicast;
mod state_machine;
mod network;
mod storage;

lazy_static! {
    static ref THIS_GRAPH_NODE :RaftNode= {
        RaftNode {
            id:config::CONFIG.raftConfig.nodeId,
            httpAddr:config::CONFIG.raftConfig.httpAddr.clone(),
            rpcAddr:config::CONFIG.raftConfig.rpcAddr.clone()
        }
    };

    static ref ONLINE_RAFT_ID_RAFT_NODE: RwLock<HashMap<GraphRaftNodeId, RaftNode>> = Default::default();

    static ref RAFT_STORE: TrickyContainer<DB> = TrickyContainer::new();
}

pub type GraphRaftNodeId = u64;
type OpenRaftConfig = openraft::Config;

#[derive(Deserialize, Default, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct RaftNode {
    pub id: GraphRaftNodeId,
    pub httpAddr: String,
    pub rpcAddr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set(Vec<(Vec<Byte>, Vec<Byte>)>)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: (),
}

openraft::declare_raft_types!(
    pub GraphRaftTypeConfig:
        D = Request,
        R = Response,
        Node = RaftNode,
        NodeId = GraphRaftNodeId,
);

pub async fn initRaft() -> Result<()> {
    let openRaftConfig = OpenRaftConfig {
        heartbeat_interval: 250,
        election_timeout_min: 200,
        ..Default::default()
    };

    let openRaftConfig = Arc::new(openRaftConfig.validate()?);

    initRaftStore()?;

    Ok(())
}

const COLUMN_FAMILY_NAME_LOG_ENTRIES: &str = "log_entries";

fn getRaftColumnFamily(columnFamilyName: &str) -> Result<ColumnFamily<'static>> {
    match RAFT_STORE.cf_handle(columnFamilyName) {
        Some(cf) => Ok(cf),
        None => throwFormat!("column family:{} not exist", columnFamilyName)
    }
}

fn initRaftStore() -> Result<()> {
    let mut raftStoreOptions = Options::default();
    raftStoreOptions.set_keep_log_file_num(1);
    raftStoreOptions.create_if_missing(true);
    raftStoreOptions.create_missing_column_families(true);

    // "default"的column family是不用显式指明的
    RAFT_STORE.set(DB::open_cf(&raftStoreOptions, config::CONFIG.raftConfig.dir.as_str(), vec![COLUMN_FAMILY_NAME_LOG_ENTRIES])?);

    Ok(())
}

#[cfg(test)]
mod test {
    use rocksdb::{DB, Options};
    use crate::config;
    use crate::raft::{COLUMN_FAMILY_NAME_LOG_ENTRIES, Request};

    #[test]
    pub fn testJsonByte() {
        let request = Request::Set(vec![(vec![7], vec![7])]);
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    pub fn testInitRaftStore() {
        let mut raftStoreOptions = Options::default();
        raftStoreOptions.set_keep_log_file_num(1);
        raftStoreOptions.create_if_missing(true);
        raftStoreOptions.create_missing_column_families(true);

        let db: DB = DB::open_cf(&raftStoreOptions, "test", vec![COLUMN_FAMILY_NAME_LOG_ENTRIES]).unwrap();
        db.put(&[0], &[1]).unwrap();
    }
}