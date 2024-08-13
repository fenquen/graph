use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use openraft::{EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use openraft::storage::RaftStateMachine;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use crate::raft;
use crate::raft::{GraphRaftNode, GraphRaftNodeId, GraphRaftTypeConfig, OpenRaftEntry, GraphRaftResponse, StorageResult, GraphRaftRequest};
use crate::session::Session;
use crate::types::Byte;

/// 其实使用openRaft的Snapshot就可以了,然而它不支持 Serialize Deserialize,只能在写个废话的
/// 保存到rocksdb的是这个
#[derive(Serialize, Deserialize, Debug, Clone)]
struct GraphRaftSnapshot {
    snapshotMeta: SnapshotMeta<GraphRaftNodeId, GraphRaftNode>,
    snapshotData: Vec<Byte>,
}

#[derive(Clone)]
pub struct GraphRaftStateMachine {
    lastAppliedLogId: Option<LogId<GraphRaftNodeId>>,
    lastMembership: StoredMembership<GraphRaftNodeId, GraphRaftNode>,
    snapshotIndex: u64,
    raftStore: &'static DB,
}

impl GraphRaftStateMachine {
    const KEY_SNAPSHOT: &'static [u8] = b"snapshot";

    pub fn new(db: Arc<DB>) -> StorageResult<GraphRaftStateMachine> {
        let mut graphRaftStateMachine = GraphRaftStateMachine {
            lastAppliedLogId: None,
            lastMembership: Default::default(),
            snapshotIndex: u64::default(),
            raftStore: &raft::RAFT_STORE,
        };

        if let Some(ref storedSnapshot) = graphRaftStateMachine.readSnapshot()? {
            graphRaftStateMachine.applySnapshot(storedSnapshot)?;
        }

        Ok(graphRaftStateMachine)
    }

    fn readSnapshot(&self) -> StorageResult<Option<GraphRaftSnapshot>> {
        match self.raftStore.get(Self::KEY_SNAPSHOT).map_err(|e| StorageIOError::read(&e))? {
            Some(value) => {
                let graphSnapshot: GraphRaftSnapshot = serde_json::from_slice(&value).map_err(|e| StorageIOError::read(&e))?;
                Ok(Some(graphSnapshot))
            }
            None => Ok(None)
        }
    }

    fn saveSnapshot(&self, graphSnapshot: &GraphRaftSnapshot) -> StorageResult<()> {
        let value =
            serde_json::to_vec(graphSnapshot).map_err(|e| StorageIOError::write_snapshot(Some(graphSnapshot.snapshotMeta.signature()), &e))?;

        self.raftStore.put(Self::KEY_SNAPSHOT, &value).map_err(|e| StorageIOError::write_snapshot(Some(graphSnapshot.snapshotMeta.signature()), &e))?;

        // self.flush(ErrorSubject::Snapshot(Some(storedSnapshot.snapshotMeta.signature())), ErrorVerb::Write)?;

        Ok(())
    }

    fn applySnapshot(&mut self, graphRaftSnapshot: &GraphRaftSnapshot) -> StorageResult<()> {
        self.lastAppliedLogId = graphRaftSnapshot.snapshotMeta.last_log_id;
        self.lastMembership = graphRaftSnapshot.snapshotMeta.last_membership.clone();
        Ok(())
    }
}

impl RaftSnapshotBuilder<GraphRaftTypeConfig> for GraphRaftStateMachine {
    /// 读取当前在内存中相应的信息,保存到rocksdb然后发送上报
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<GraphRaftTypeConfig>> {
        let lastAppliedLogId = self.lastAppliedLogId;
        let lastMembership = self.lastMembership.clone();

        // let mapJsonByteVec = serde_json::to_vec(&*self.kv.read().await).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshotId =
            if let Some(lastAppliedLogId) = lastAppliedLogId {
                format!("{}-{}-{}", lastAppliedLogId.leader_id, lastAppliedLogId.index, self.snapshotIndex)
            } else {
                format!("{}", self.snapshotIndex)
            };

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: lastAppliedLogId,
                last_membership: lastMembership,
                snapshot_id: snapshotId,
            },
            snapshot: Box::default(),
        };

        // 保存到rocksdb
        self.saveSnapshot(&GraphRaftSnapshot {
            snapshotMeta: snapshot.meta.clone(),
            snapshotData: Vec::new(),
        })?;

        // 上报
        Ok(snapshot)
    }
}

impl RaftStateMachine<GraphRaftTypeConfig> for GraphRaftStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> StorageResult<(Option<LogId<GraphRaftNodeId>>, StoredMembership<GraphRaftNodeId, GraphRaftNode>)> {
        Ok((self.lastAppliedLogId, self.lastMembership.clone()))
    }

    /// logReader的try_get_log_entries得到多个log 然后作为该函数原料
    /// db的变化体现在了这些entry
    async fn apply<I>(&mut self, openRaftEntries: I) -> StorageResult<Vec<GraphRaftResponse>>
    where
        I: IntoIterator<Item=OpenRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let openRaftEntries = openRaftEntries.into_iter();
        let mut replies = Vec::with_capacity(openRaftEntries.size_hint().0);

        for openRaftEntry in openRaftEntries {
            self.lastAppliedLogId = Some(openRaftEntry.log_id);


            match openRaftEntry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(graphRaftRequest) => match graphRaftRequest {
                    GraphRaftRequest::Set(dbObjectId_keyValues) => {
                        for (dbObjectId, keyValues) in dbObjectId_keyValues {
                            let columnFamily = Session::getColumnFamily(dbObjectId).map_err(|e| StorageIOError::apply(openRaftEntry.log_id, e))?;


                        }
                        //self.kv.write().await.insert(key, value);
                    }
                },
                EntryPayload::Membership(membership) => {
                    self.lastMembership = StoredMembership::new(Some(openRaftEntry.log_id), membership);
                }
            }

            replies.push(GraphRaftResponse::default());
        }

        Ok(replies)
    }

    // 要增加1的原因是,调用了该函数后会立即调用SnapshotBuilder的build_snapshot()
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshotIndex += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<<GraphRaftTypeConfig as RaftTypeConfig>::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self,
                              snapshotMeta: &SnapshotMeta<GraphRaftNodeId, GraphRaftNode>,
                              snapshotData: Box<Cursor<Vec<u8>>>) -> StorageResult<()> {
        let graphRaftSnapshot = GraphRaftSnapshot {
            snapshotMeta: snapshotMeta.clone(),
            snapshotData: snapshotData.into_inner(),
        };

        // 内存里边的修改
        self.applySnapshot(&graphRaftSnapshot)?;

        // 持久化的记录的 对应下边的get_current_snapshot
        self.saveSnapshot(&graphRaftSnapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<GraphRaftTypeConfig>>> {
        match self.readSnapshot()? {
            Some(graphSnapshot) => Ok(Some(Snapshot {
                meta: graphSnapshot.snapshotMeta.clone(),
                snapshot: Box::new(Cursor::new(graphSnapshot.snapshotData.clone())),
            })),
            None => Ok(None)
        }
    }
}