use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use crate::config;
use crate::types::RaftNodeId;

pub mod multicast;

lazy_static! {
    pub static ref THIS_GRAPH_NODE :GraphRaftNode= {
        GraphRaftNode {
            id:config::CONFIG.raftConfig.nodeId,
            httpAddr:config::CONFIG.raftConfig.httpAddr.clone(),
            rpcAddr:config::CONFIG.raftConfig.rpcAddr.clone()
        }
    };
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GraphRaftNode {
    pub id: RaftNodeId,
    pub httpAddr: String,
    pub rpcAddr: String,
}