use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, Infallible};
use openraft::RaftMetrics;
use tide::Body;
use tide::Response;
use tide::StatusCode;

use crate::Application;
use crate::Node;
use crate::NodeId;
use crate::types::{TideHttpRequest, TideHttpServer};

/// http service
pub fn rest(tideHttpServer: &mut TideHttpServer<Arc<Application>>) {
    let mut api = tideHttpServer.at("/api");
    api.at("/write").post(write);
    api.at("/read").post(read);
    api.at("/consistent_read").post(consistent_read);

    let mut cluster = tideHttpServer.at("/cluster");
    cluster.at("/add-learner").post(add_learner);
    cluster.at("/change-membership").post(change_membership);
    cluster.at("/init").post(init);
    cluster.at("/metrics").get(metrics);
}

async fn write(mut httpRequest: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let request = httpRequest.body_json().await?;
    let res = httpRequest.state().raft.client_write(request).await;
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}

async fn read(mut request: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let key: String = request.body_json().await?;
    let key_value = request.state().key_value.read().await;
    let value = key_value.get(&key);

    let res: Result<String, Infallible> = Ok(value.cloned().unwrap_or_default());
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}

async fn consistent_read(mut request: TideHttpRequest<Arc<Application>>) -> tide::Result {
    match request.state().raft.ensure_linearizable().await {
        Ok(_) => {
            let key: String = request.body_json().await?;
            let kvs = request.state().key_value.read().await;

            let value = kvs.get(&key);

            let res: Result<String, CheckIsLeaderError<NodeId, Node>> = Ok(value.cloned().unwrap_or_default());
            Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
        }
        e => Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&e)?).build()),
    }
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
async fn add_learner(mut req: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let (node_id, api_addr, rpc_addr): (NodeId, String, String) = req.body_json().await?;

    let node = Node {
        rpcAddr: rpc_addr,
        httpAddr: api_addr,
    };

    let res = req.state().raft.add_learner(node_id, node, true).await;
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}

/// Changes specified learners to members, or remove members.
async fn change_membership(mut req: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let body: BTreeSet<NodeId> = req.body_json().await?;
    let res = req.state().raft.change_membership(body, false).await;
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}

/// Initialize a single-node cluster.
async fn init(tideHttpRequest: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let mut nodes = BTreeMap::new();

    let node = Node {
        httpAddr: tideHttpRequest.state().httpAddr.clone(),
        rpcAddr: tideHttpRequest.state().rpcAddr.clone(),
    };

    nodes.insert(tideHttpRequest.state().nodeId, node);

    let res = tideHttpRequest.state().raft.initialize(nodes).await;
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}

/// Get the latest metrics of the cluster
async fn metrics(req: TideHttpRequest<Arc<Application>>) -> tide::Result {
    let metrics = req.state().raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<NodeId, Node>, Infallible> = Ok(metrics);
    Ok(Response::builder(StatusCode::Ok).body(Body::from_json(&res)?).build())
}
