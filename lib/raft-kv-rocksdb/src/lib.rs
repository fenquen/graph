#![allow(clippy::uninlined_format_args, non_snake_case)]
#![allow(unused_imports)]

use std::fmt::Display;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task;
use tokio::sync::RwLock;
use std::collections::BTreeMap;
use impls::network::RaftNetworkFactoryImpl;
use impls::network::RpcEndpoint;
use types::{NodeId, TideHttpServer};
use crate::types::OpenRaftConfig;

pub mod http_client;
pub mod types;
mod impls;
pub mod http_server;

/// represent an application state.
pub struct Application {
    pub nodeId: NodeId,
    pub httpAddr: String,
    pub rpcAddr: String,
    pub raft: openraft::Raft<RaftTypeConfigImpl>,
    pub key_value: Arc<RwLock<BTreeMap<String, String>>>,
    pub raftConfig: Arc<OpenRaftConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    /// used to write data (key and value) to the raft database
    Set {
        key: String,
        value: String,
    },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in the `ExampleRequest.Set`
 *
 * TODO: Should we explain how to create multiple `AppDataResponse`?
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpcAddr: String,
    pub httpAddr: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node {{ rpc_addr: {}, api_addr: {} }}", self.rpcAddr, self.httpAddr)
    }
}

openraft::declare_raft_types!(
    pub RaftTypeConfigImpl:
        D = Request,
        R = Response,
        Node = Node,
);

pub async fn startRaftNode(nodeId: NodeId,
                           dirPath: impl AsRef<Path>,
                           httpAddr: &str,
                           rpcAddr: &str) -> std::io::Result<()> {
    let openRaftConfig = OpenRaftConfig {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        ..Default::default()
    };

    let openRaftConfig = Arc::new(openRaftConfig.validate().unwrap());

    let (raftStorage, raftStateMachine) = impls::newStorageAndStateMachine(&dirPath).await;

    let kv = raftStateMachine.kv.clone();

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = RaftNetworkFactoryImpl;

    // Create a local raft instance.
    let raft =
        openraft::Raft::new(nodeId,
                            openRaftConfig.clone(),
                            network,
                            raftStorage,
                            raftStateMachine).await.unwrap();

    let app = Arc::new(
        Application {
            nodeId,
            httpAddr: httpAddr.to_string(),
            rpcAddr: rpcAddr.to_string(),
            raft,
            key_value: kv,
            raftConfig: openRaftConfig,
        });

    // rpc use in communication among raft node
    let rpcServer = toy_rpc::Server::builder().register(Arc::new(RpcEndpoint::new(app.clone()))).build();
    let listener = TcpListener::bind(rpcAddr).await.unwrap();
    let handle = task::spawn(async move {
        rpcServer.accept_websocket(listener).await.unwrap();
    });

    // http
    let mut tideServer: TideHttpServer<Arc<Application>> = TideHttpServer::with_state(app);
    http_server::rest(&mut tideServer);
    tideServer.listen(httpAddr).await?;

    tracing::info!("App Server listening on: {}", httpAddr);
    _ = handle.await;

    Ok(())
}