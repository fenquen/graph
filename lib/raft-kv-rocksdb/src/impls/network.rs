use std::any::Any;
use std::fmt::Display;

use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::AnyError;
use serde::de::DeserializeOwned;
use toy_rpc::pubsub::AckModeNone;
use toy_rpc::macros::export_impl;
use std::sync::Arc;
use crate::{Application, Node};
use crate::NodeId;
use crate::RaftTypeConfigImpl;
use crate::types::{OpenRaftRPCError, ToyRpcClient};

/// 用来应对其它节点的rpc请求
pub struct RaftRpcEndpoint {
    application: Arc<Application>,
}

#[export_impl]
impl RaftRpcEndpoint {
    pub fn new(application: Arc<Application>) -> Self {
        Self { application }
    }

    #[export_method]
    pub async fn append(&self, req: AppendEntriesRequest<RaftTypeConfigImpl>) -> Result<AppendEntriesResponse<u64>, toy_rpc::Error> {
        tracing::debug!("handle append");
        self.application.raft.append_entries(req).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn installSnapshot(&self, req: InstallSnapshotRequest<RaftTypeConfigImpl>) -> Result<InstallSnapshotResponse<u64>, toy_rpc::Error> {
        self.application.raft.install_snapshot(req).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn vote(&self, vote: VoteRequest<u64>) -> Result<VoteResponse<u64>, toy_rpc::Error> {
        self.application.raft.vote(vote).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
}

pub struct RaftNetworkFactoryImpl;

// could be implemented also on `Arc<ExampleNetwork>`, since it's an empty struct, implemented directly.
impl RaftNetworkFactory<RaftTypeConfigImpl> for RaftNetworkFactoryImpl {
    type Network = RaftNetworkImpl;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, targetNodeId: NodeId, node: &Node) -> Self::Network {
        let targetNodeRpcAddr = format!("ws://{}", node.rpcAddr);

        let rpcClient = ToyRpcClient::dial_websocket(&targetNodeRpcAddr).await.ok();

        RaftNetworkImpl {
            targetNodeId,
            targetNodeRpcAddr,
            rpcClient,
        }
    }
}

pub struct RaftNetworkImpl {
    targetNodeId: NodeId,
    targetNodeRpcAddr: String,
    rpcClient: Option<ToyRpcClient<AckModeNone>>,
}

impl RaftNetworkImpl {
    async fn getRpcClient<E: std::error::Error + DeserializeOwned>(&mut self) -> Result<&ToyRpcClient<AckModeNone>, RPCError<NodeId, Node, E>> {
        if self.rpcClient.is_none() {
            self.rpcClient = ToyRpcClient::dial_websocket(&self.targetNodeRpcAddr).await.ok();
        }

        self.rpcClient.as_ref().ok_or_else(|| RPCError::Network(NetworkError::from(AnyError::default())))
    }
}

#[derive(Debug)]
struct ErrWrap(Box<dyn std::error::Error>);

impl Display for ErrWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ErrWrap {}

#[allow(clippy::blocks_in_conditions)]
impl RaftNetwork<RaftTypeConfigImpl> for RaftNetworkImpl {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(&mut self,
                            appendEntriesRequest: AppendEntriesRequest<RaftTypeConfigImpl>,
                            _option: RPCOption) -> Result<AppendEntriesResponse<NodeId>, OpenRaftRPCError<RaftError<NodeId>>> {
        tracing::debug!(req = debug(&appendEntriesRequest), "append_entries");
        self.getRpcClient().await?.raft_rpc_endpoint().append(appendEntriesRequest).await.map_err(|e| toyRpcError2OpenRaftError(e, self.targetNodeId))
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn install_snapshot(&mut self,
                              installSnapshotRequest: InstallSnapshotRequest<RaftTypeConfigImpl>,
                              _option: RPCOption) -> Result<InstallSnapshotResponse<NodeId>, OpenRaftRPCError<RaftError<NodeId, InstallSnapshotError>>> {
        tracing::debug!(req = debug(&installSnapshotRequest), "install_snapshot");
        self.getRpcClient().await?.raft_rpc_endpoint().installSnapshot(installSnapshotRequest).await.map_err(|e| toyRpcError2OpenRaftError(e, self.targetNodeId))
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(&mut self,
                  voteReuest: VoteRequest<NodeId>,
                  _option: RPCOption) -> Result<VoteResponse<NodeId>, OpenRaftRPCError<RaftError<NodeId>>> {
        tracing::debug!(req = debug(&voteReuest), "vote");
        self.getRpcClient().await?.raft_rpc_endpoint().vote(voteReuest).await.map_err(|e| toyRpcError2OpenRaftError(e, self.targetNodeId))
    }
}

fn toyRpcError2OpenRaftError<E: std::error::Error + 'static + Clone>(e: toy_rpc::Error, target: NodeId) -> OpenRaftRPCError<E> {
    match e {
        toy_rpc::Error::IoError(e) => OpenRaftRPCError::Network(NetworkError::new(&e)),
        toy_rpc::Error::ParseError(e) => OpenRaftRPCError::Network(NetworkError::new(&ErrWrap(e))),
        toy_rpc::Error::Internal(e) => {
            let any: &dyn Any = &e;
            let error: &E = any.downcast_ref().unwrap();
            OpenRaftRPCError::RemoteError(RemoteError::new(target, error.clone()))
        }
        _ => OpenRaftRPCError::Network(NetworkError::new(&e)),
    }
}