use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::{Config, Raft};
use tokio::sync::RwLock;

use crate::NodeId;
use crate::TypeConfig;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: Raft<TypeConfig>,
    pub key_values: Arc<RwLock<BTreeMap<String, String>>>,
    pub config: Arc<Config>,
}
