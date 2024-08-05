use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use openraft::{AnyError, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftSnapshotBuilder};
use openraft::{Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use openraft::storage::RaftStateMachine;
use rocksdb::DB;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::{Node, RaftTypeConfigImpl, Request, types};
use crate::Response;
use crate::types::{ColumnFamily, NodeId, StorageResult};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StoredSnapshot {
    pub snapshotMeta: SnapshotMeta<NodeId, Node>,

    /// The data of the state machine at the time of this snapshot<br>
    /// 目前传递的是 BTreeMap<String, String>的json的binary
    pub data: Vec<u8>,
}

/// 实现了 RaftSnapshotBuilder , RaftStateMachine
#[derive(Debug, Clone)]
pub struct RaftStateMachineImpl {
    pub lastAppliedLogId: Option<LogId<NodeId>>,
    pub lastMembership: StoredMembership<NodeId, Node>,

    /// state built from applying the raft logs
    pub kv: Arc<RwLock<BTreeMap<String, String>>>,

    /// snapshot index is not persisted in this example.
    ///
    /// It is only used as a suffix of snapshot id string, and should be globally unique.
    /// In practice, using a timestamp in micro-second would be good enough.
    snapshotIndex: u64,

    /// stores snapshot in db.
    db: Arc<DB>,
}

impl RaftStateMachineImpl {
    pub async fn new(db: Arc<DB>) -> Result<RaftStateMachineImpl, StorageError<NodeId>> {
        let mut raftStateMachineImpl = RaftStateMachineImpl {
            lastAppliedLogId: None,
            lastMembership: Default::default(),
            kv: Default::default(),
            snapshotIndex: u64::default(),
            db,
        };

        if let Some(storedSnapshot) = raftStateMachineImpl.readSnapshot()? {
            raftStateMachineImpl.applySnapshot(storedSnapshot).await?;
        }

        Ok(raftStateMachineImpl)
    }

    fn readSnapshot(&self) -> StorageResult<Option<StoredSnapshot>> {
        Ok(self.db.get_cf(&self.getStoreCF(), b"snapshot")
            .map_err(|e| StorageError::IO { source: StorageIOError::read(&e) })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn saveSnapshot(&self, storedSnapshot: StoredSnapshot) -> StorageResult<()> {
        self.db.put_cf(&self.getStoreCF(), b"snapshot", serde_json::to_vec(&storedSnapshot).unwrap().as_slice())
            .map_err(|e| StorageError::IO { source: StorageIOError::write_snapshot(Some(storedSnapshot.snapshotMeta.signature()), &e) })?;

        self.flush(ErrorSubject::Snapshot(Some(storedSnapshot.snapshotMeta.signature())), ErrorVerb::Write)?;

        Ok(())
    }

    async fn applySnapshot(&mut self, storedSnapshot: StoredSnapshot) -> StorageResult<()> {
        let kv: BTreeMap<String, String> =
            serde_json::from_slice(&storedSnapshot.data)
                .map_err(|e| StorageIOError::read_snapshot(Some(storedSnapshot.snapshotMeta.signature()), &e))?;

        self.lastAppliedLogId = storedSnapshot.snapshotMeta.last_log_id;
        self.lastMembership = storedSnapshot.snapshotMeta.last_membership.clone();

        let mut x = self.kv.write().await;
        *x = kv;

        Ok(())
    }

    fn getStoreCF(&self) -> ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }

    fn flush(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> Result<(), StorageIOError<NodeId>> {
        self.db.flush_wal(true).map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<RaftTypeConfigImpl> for RaftStateMachineImpl {
    /// 读取当前在内存中的snapshot相应的信息,保存到rocksdb然后发送上报
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftTypeConfigImpl>, StorageError<NodeId>> {
        let lastAppliedLogId = self.lastAppliedLogId;
        let lastMembership = self.lastMembership.clone();

        let mapJsonByteVec = serde_json::to_vec(&*self.kv.read().await).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshotId =
            if let Some(lastAppliedLogId) = lastAppliedLogId {
                format!("{}-{}-{}", lastAppliedLogId.leader_id, lastAppliedLogId.index, self.snapshotIndex)
            } else {
                format!("{}", self.snapshotIndex)
            };

        let snapshotMeta = SnapshotMeta {
            last_log_id: lastAppliedLogId,
            last_membership: lastMembership,
            snapshot_id: snapshotId,
        };

        // 保存到rocksdb
        self.saveSnapshot(StoredSnapshot {
            snapshotMeta: snapshotMeta.clone(),
            data: mapJsonByteVec.clone(),
        })?;

        // 上报
        Ok(Snapshot {
            meta: snapshotMeta,
            snapshot: Box::new(Cursor::new(mapJsonByteVec)),
        })
    }
}

impl RaftStateMachine<RaftTypeConfigImpl> for RaftStateMachineImpl {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> StorageResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>)> {
        Ok((self.lastAppliedLogId, self.lastMembership.clone()))
    }

    /// logReader的try_get_log_entries得到多个log 然后作为该函数原料
    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<Response>>
    where
        I: IntoIterator<Item=types::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for entry in entries {
            self.lastAppliedLogId = Some(entry.log_id);

            let mut resp_value = None;

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(request) => match request {
                    Request::Set { key, value } => {
                        resp_value = Some(value.clone());
                        self.kv.write().await.insert(key, value);
                    }
                },
                EntryPayload::Membership(mem) => {
                    self.lastMembership = StoredMembership::new(Some(entry.log_id), mem);
                }
            }

            replies.push(Response { value: resp_value });
        }

        Ok(replies)
    }

    // 要增加1的原因是,调用了该函数后会立即调用SnapshotBuilder的build_snapshot()
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshotIndex += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self,
                              snapshotMeta: &SnapshotMeta<NodeId, Node>,
                              snapshotData: Box<Cursor<Vec<u8>>>) -> StorageResult<()> {
        let storedSnapshot = StoredSnapshot {
            snapshotMeta: snapshotMeta.clone(),
            data: snapshotData.into_inner(),
        };

        self.applySnapshot(storedSnapshot.clone()).await?;

        self.saveSnapshot(storedSnapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<RaftTypeConfigImpl>>> {
        Ok(self.readSnapshot()?.map(|storedSnapshot| Snapshot {
            meta: storedSnapshot.snapshotMeta.clone(),
            snapshot: Box::new(Cursor::new(storedSnapshot.data.clone())),
        }))
    }
}