use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use openraft::RaftMetrics;
use openraft::TryAsRef;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::types;
use crate::Node;
use crate::NodeId;
use crate::Request;
use crate::types::{ClientWriteResponse, ForwardToLeader, OpenRaftCheckIsLeaderError, OpenRaftClientWriteError, OpenRaftRaftError, OpenRaftRPCError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct HttpClient {
    /// 起始的时候可以先随便填写,后续会更新成为真正的leader地址
    pub leader: Arc<Mutex<(NodeId, String)>>,
    pub httpClient: Client,
}

impl HttpClient {
    pub fn new(targetNodeId: NodeId, targetHttpAddr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((targetNodeId, targetHttpAddr))),
            httpClient: Client::new(),
        }
    }

    // ------------------------- read/write-------------------------------------
    pub async fn write(&self, request: &Request) -> Result<ClientWriteResponse, OpenRaftRPCError<OpenRaftClientWriteError>> {
        self.sendHttpRequest2Leader("api/write", Some(request)).await
    }

    /// Read value by key in inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    pub async fn read(&self, key: &String) -> Result<String, OpenRaftRPCError> {
        self.sendHttpRequest("api/read", Some(key)).await
    }

    /// Consistent Read value by key in an consistent mode.
    ///
    /// This method MUST return consistent value or CheckIsLeaderError.
    pub async fn consistentRead(&self, key: &String) -> Result<String, OpenRaftRPCError<OpenRaftCheckIsLeaderError>> {
        self.sendHttpRequest("api/consistent_read", Some(key)).await
    }

    // ------------------- cluster management---------------------------------------------

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added data with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(&self) -> Result<(), OpenRaftRPCError<types::InitializeError>> {
        self.sendHttpRequest("cluster/init", Some(&Empty {})).await
    }

    /// add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    pub async fn addLeaner(&self, learner: (NodeId, String, String)) -> Result<ClientWriteResponse, OpenRaftRPCError<OpenRaftClientWriteError>> {
        self.sendHttpRequest2Leader("cluster/add-learner", Some(&learner)).await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn changeMembership(&self, newMembership: &BTreeSet<NodeId>) -> Result<ClientWriteResponse, OpenRaftRPCError<OpenRaftClientWriteError>> {
        self.sendHttpRequest2Leader("cluster/change-membership", Some(newMembership)).await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader, membership config, replication status etc.
    /// See [`RaftMetrics`]
    pub async fn metrics(&self) -> Result<RaftMetrics<NodeId, Node>, OpenRaftRPCError> {
        self.sendHttpRequest("cluster/metrics", None::<&()>).await
    }

    // -------------------------------------- internal ------------------------------------

    /// try to send to the leader,
    /// if the target node is not a leader, error will be returned ,then will retry
    async fn sendHttpRequest2Leader<Req, Resp, Err>(&self, uri: &str, req: Option<&Req>) -> Result<Resp, OpenRaftRPCError<Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned + TryAsRef<ForwardToLeader> + Clone,
    {
        let mut maxRetry = 3usize;

        loop {
            let res: Result<Resp, OpenRaftRPCError<Err>> = self.sendHttpRequest(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let raft_err: &OpenRaftRaftError<_> = &remote_err.source;

                if let Some(ForwardToLeader {
                                leader_id: Some(leaderNodeId),
                                leader_node: Some(leaderNode),
                                ..
                            }) = raft_err.forward_to_leader() {

                    // 更新最新的leader信息
                    {
                        let mut leader = self.leader.lock().unwrap();
                        *leader = (*leaderNodeId, leaderNode.httpAddr.clone());
                    }

                    maxRetry -= 1;

                    if maxRetry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }

    /// Send RPC to specified node.
    ///
    /// It sends out a POST request if `req` is Some. Otherwise a GET request.
    /// The remote endpoint must respond a reply in form of `Result<T, E>`.
    /// An `Err` happened on remote will be wrapped in an [`RPCError::RemoteError`].
    async fn sendHttpRequest<Req, Resp, Err>(&self, serveletPath: &str, req: Option<&Req>) -> Result<Resp, RPCError<NodeId, Node, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leaderNodeId, leaderHttpUrl) = {
            let leader = self.leader.lock().unwrap();
            (leader.0, format!("http://{}/{}",  &leader.1, serveletPath))
        };

        let resp =
            if let Some(r) = req {
                println!("client send request to {}: {}", leaderHttpUrl, serde_json::to_string_pretty(&r).unwrap());
                self.httpClient.post(leaderHttpUrl.clone()).json(r)
            } else {
                println!("client send request to {}", leaderHttpUrl, );
                self.httpClient.get(leaderHttpUrl.clone())
            }.send().await.map_err(|e| {
                if e.is_connect() {
                    // `Unreachable` informs the caller to backoff for a short while to avoid error log flush.
                    return RPCError::Unreachable(Unreachable::new(&e));
                }

                RPCError::Network(NetworkError::new(&e))
            })?;

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        println!("client recv reply from {}: {}", leaderHttpUrl, serde_json::to_string_pretty(&res).unwrap());

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leaderNodeId, e)))
    }
}
