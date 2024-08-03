#![allow(non_snake_case)]
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::panic::{PanicHookInfo};
use std::{fs, thread};
use std::path::Path;
use std::time::Duration;

use maplit::btreemap;
use maplit::btreeset;
use raft_kv_rocksdb::http_client::HttpClient;
use raft_kv_rocksdb::startRaftNode;
use raft_kv_rocksdb::Request;
use raft_kv_rocksdb::Node;
use tokio::runtime::Handle;
use tracing_subscriber::EnvFilter;
use raft_kv_rocksdb::types::NodeId;

fn logPanic(panicHookInfo: &PanicHookInfo) {
    let backtrace = { format!("{:?}", Backtrace::force_capture()) };

    eprintln!("{}", panicHookInfo);

    if let Some(location) = panicHookInfo.location() {
        tracing::error!(
            message = %panicHookInfo,
            backtrace = %backtrace,
            panic.file = location.file(),
            panic.line = location.line(),
            panic.column = location.column(),
        );
        eprintln!("{}:{}:{}", location.file(), location.line(), location.column());
    } else {
        tracing::error!(message = %panicHookInfo, backtrace = %backtrace);
    }

    eprintln!("{}", backtrace);
}

const NODE_ID_1: u64 = 1;
const NODE_ID_2: u64 = 2;
const NODE_ID_3: u64 = 3;

const HTTP_ADDR_1: &str = "127.0.0.1:31001";
const HTTP_ADDR_2: &str = "127.0.0.1:31002";
const HTTP_ADDR_3: &str = "127.0.0.1:31003";

const RPC_ADDR_1: &str = "127.0.0.1:33001";
const RPC_ADDR_2: &str = "127.0.0.1:33002";
const RPC_ADDR_3: &str = "127.0.0.1:33003";

const DATA_DIR_PATH_1: &str = "node1";
const DATA_DIR_PATH_2: &str = "node2";
const DATA_DIR_PATH_3: &str = "node3";

/// setup a cluster of 3 nodes,write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster() -> Result<(), Box<dyn std::error::Error>> {
    std::panic::set_hook(Box::new(|panicHookInfo| { logPanic(panicHookInfo); }));

    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if Path::new(DATA_DIR_PATH_1).exists() {
        fs::remove_dir_all(DATA_DIR_PATH_1)?;
    }
    fs::create_dir_all(DATA_DIR_PATH_1)?;

    if Path::new(DATA_DIR_PATH_2).exists() {
        fs::remove_dir_all(DATA_DIR_PATH_2)?;
    }
    fs::create_dir_all(DATA_DIR_PATH_2)?;

    if Path::new(DATA_DIR_PATH_3).exists() {
        fs::remove_dir_all(DATA_DIR_PATH_3)?;
    }
    fs::create_dir_all(DATA_DIR_PATH_3)?;

    let handle = Handle::current();
    let handle_clone = handle.clone();
    let _h1 = thread::spawn(move || {
        println!("x: {:?}", handle_clone.block_on(startRaftNode(NODE_ID_1, DATA_DIR_PATH_1, HTTP_ADDR_1, RPC_ADDR_1)));
    });

    let handle_clone = handle.clone();
    let _h2 = thread::spawn(move || {
        println!("x: {:?}", handle_clone.block_on(startRaftNode(NODE_ID_2, DATA_DIR_PATH_2, HTTP_ADDR_2, RPC_ADDR_2)));
    });

    let _h3 = thread::spawn(move || {
        println!("x: {:?}", handle.block_on(startRaftNode(NODE_ID_3, DATA_DIR_PATH_3, HTTP_ADDR_3, RPC_ADDR_3)));
    });

    // wait for server to start up
    tokio::time::sleep(Duration::from_millis(3000)).await;

    // create a client to the first node, as a control handle to the cluster.
    let httpClient1 = HttpClient::new(NODE_ID_1, HTTP_ADDR_1.to_string());

    // 1 Initialize the target node as a cluster of only one node.
    // After init(), the single node cluster will be fully functional.
    println!("\n init single node cluster");
    httpClient1.init().await?;

    // 2 Add node 2 and 3 to the cluster as `Learner`, to let them start to receive log replication from the leader.
    println!("\n add learner 2");
    httpClient1.addLeaner((NODE_ID_2, HTTP_ADDR_2.to_string(), RPC_ADDR_2.to_string())).await?;

    println!("\n add learner 3");
    httpClient1.addLeaner((NODE_ID_3, HTTP_ADDR_3.to_string(), RPC_ADDR_3.to_string())).await?;

    println!("\n metrics after add-learner");
    let x = httpClient1.metrics().await?;
    assert_eq!(&vec![btreeset![1]], x.membership_config.membership().get_joint_config());
    let clusterNodes: BTreeMap<NodeId, Node> = x.membership_config.nodes().map(|(nodeId, node)| (*nodeId, node.clone())).collect();
    assert_eq!(
        btreemap! {
            1 => Node{rpcAddr: RPC_ADDR_1.to_string(), httpAddr: HTTP_ADDR_1.to_string()},
            2 => Node{rpcAddr: RPC_ADDR_2.to_string(), httpAddr: HTTP_ADDR_2.to_string()},
            3 => Node{rpcAddr: RPC_ADDR_3.to_string(), httpAddr: HTTP_ADDR_3.to_string()},
        },
        clusterNodes
    );

    // turn the two learners to members, member node can vote or elect itself as leader.
    println!("\n change-membership to 1,2,3");
    httpClient1.changeMembership(&btreeset! {1,2,3}).await?;

    // --- After change-membership, some cluster state will be seen in the metrics.
    //
    // ```text
    // metrics: RaftMetrics {
    //   current_leader: Some(1),
    //   membership_config: EffectiveMembership {
    //        log_id: LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 },
    //        membership: Membership { learners: {}, configs: [{1, 2, 3}] }
    //   },
    //   leader_metrics: Some(LeaderMetrics { replication: {
    //     2: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 7 }) },
    //     3: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 }) }} })
    // }
    // ```
    println!("\n metrics after change-member");
    let raftMetrics = httpClient1.metrics().await?;
    assert_eq!(&vec![btreeset![1, 2, 3]], raftMetrics.membership_config.membership().get_joint_config());

    // write
    println!("\n write `foo=bar`");
    httpClient1.write(
        &Request::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        }).await?;

    // wait for a while to let the replication get done.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // read same key on every node.
    println!("\n read `foo` on node 1");
    assert_eq!("bar", httpClient1.read(&("foo".to_string())).await?);

    println!("\n read `foo` on node 2");
    let httpClient2 = HttpClient::new(NODE_ID_2, HTTP_ADDR_2.to_string());
    assert_eq!("bar", httpClient2.read(&("foo".to_string())).await?);

    println!("read `foo` on node 3");
    let httpClient3 = HttpClient::new(NODE_ID_3, HTTP_ADDR_3.to_string());
    assert_eq!("bar", httpClient3.read(&("foo".to_string())).await?);

    // --- A write to non-leader will be automatically forwarded to a known leader
    println!("write `foo=wow` on node 2");
    httpClient2.write(
        &Request::Set {
            key: "foo".to_string(),
            value: "wow".to_string(),
        }).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // read it on every node.
    println!("read `foo` on node 1");
    assert_eq!("wow", httpClient1.read(&("foo".to_string())).await?);

    println!("read `foo` on node 2");
    let client2 = HttpClient::new(NODE_ID_2, HTTP_ADDR_2.to_string());
    assert_eq!("wow", client2.read(&("foo".to_string())).await?);

    println!("read `foo` on node 3");
    let client3 = HttpClient::new(NODE_ID_3, HTTP_ADDR_3.to_string());
    assert_eq!("wow", client3.read(&("foo".to_string())).await?);

    println!("consistent_read `foo` on node 1");
    assert_eq!("wow", httpClient1.consistentRead(&"foo".to_string()).await?);

    println!("consistent_read `foo` on node 2 MUST return CheckIsLeaderError");
    match client2.consistentRead(&"foo".to_string()).await {
        Err(e) => {
            println!("{}", e.to_string());
        }
        Ok(_) => panic!("MUST return CheckIsLeaderError"),
    }

    Ok(())
}
