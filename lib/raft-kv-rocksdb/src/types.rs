use std::sync::Arc;
use openraft::error::Infallible;
use rocksdb::BoundColumnFamily;
use openraft::StorageError;
use crate::Node;
use crate::RaftTypeConfigImpl;

pub type Entry = openraft::Entry<RaftTypeConfigImpl>;

pub type OpenRaftRaftError<E = Infallible> = openraft::error::RaftError<NodeId, E>;
pub type OpenRaftRPCError<E = Infallible> = openraft::error::RPCError<NodeId, Node, OpenRaftRaftError<E>>;

pub type OpenRaftClientWriteError = openraft::error::ClientWriteError<NodeId, Node>;
pub type OpenRaftCheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, Node>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, Node>;
pub type InitializeError = openraft::error::InitializeError<NodeId, Node>;

pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<RaftTypeConfigImpl>;

pub type TideHttpServer<T> = tide::Server<T>;
pub type TideHttpRequest<State> = tide::Request<State>;

pub type NodeId = u64;

pub type ColumnFamily<'a> = Arc<BoundColumnFamily<'a>>;
pub type StorageResult<T> = Result<T, StorageError<NodeId>>;

pub type ToyRpcClient<AckMode> = toy_rpc::Client<AckMode>;

pub type OpenRaftConfig = openraft::Config;