use std::sync::RwLock;
use hashbrown::{HashMap, HashSet};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::config;
use crate::types::RaftNodeId;

pub mod multicast;

lazy_static! {
    pub static ref THIS_GRAPH_NODE :RaftNode= {
        RaftNode {
            id:config::CONFIG.raftConfig.nodeId,
            httpAddr:config::CONFIG.raftConfig.httpAddr.clone(),
            rpcAddr:config::CONFIG.raftConfig.rpcAddr.clone()
        }
    };

    pub static ref ONLINE_RAFT_ID_RAFT_NODE: RwLock<HashMap<RaftNodeId, RaftNode>> = Default::default();
}

#[derive(Deserialize, Default, Serialize, Clone, Debug)]
pub struct RaftNode {
    pub id: RaftNodeId,
    pub httpAddr: String,
    pub rpcAddr: String,
}