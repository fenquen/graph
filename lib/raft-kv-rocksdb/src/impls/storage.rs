use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode};
use rocksdb::Direction;
use rocksdb::Options;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::types;
use crate::Node;
use crate::NodeId;
use crate::RaftTypeConfigImpl;
use crate::impls::state_machine::RaftStateMachineImpl;
use crate::types::{ColumnFamily, StorageResult};

#[derive(Debug, Clone)]
pub struct RaftLogReaderStorageImpl {
    db: Arc<DB>,
}

impl RaftLogReaderStorageImpl {
    const KEY_LAST_PURGED_LOG_ID: [u8; 18] = *b"last_purged_log_id";

    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    /// 读取 store的last_purged_log_id
    fn readLastPurgedLogId(&self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self.db
            .get_cf(&self.getStoreCF(), Self::KEY_LAST_PURGED_LOG_ID)
            .map_err(|e| StorageIOError::read(&e))?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    /// 写入 store的last_purged_log_id
    fn saveLastPurgedLogId(&self, logId: LogId<u64>) -> StorageResult<()> {
        let key = Self::KEY_LAST_PURGED_LOG_ID;
        let value = serde_json::to_vec(&logId).unwrap();
        self.db.put_cf(&self.getStoreCF(), key, value).map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;

        Ok(())
    }

    /// 读取 store的committed
    fn readCommitted(&self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.db.get_cf(&self.getStoreCF(), b"committed")
            .map_err(|e| StorageError::IO { source: StorageIOError::read(&e) })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    /// 写入 store的committed
    fn saveCommitted(&self, committed: &Option<LogId<NodeId>>) -> Result<(), StorageIOError<NodeId>> {
        let value = serde_json::to_vec(committed).unwrap();

        self.db.put_cf(&self.getStoreCF(), b"committed", value).map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;

        Ok(())
    }

    /// 读取 store的vote
    fn readVote(&self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.db.get_cf(&self.getStoreCF(), b"vote")
            .map_err(|e| StorageError::IO { source: StorageIOError::write_vote(&e) })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    /// 写入 store的vote
    fn saveVote(&self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.db.put_cf(&self.getStoreCF(), b"vote", serde_json::to_vec(vote).unwrap())
            .map_err(|e| StorageError::IO { source: StorageIOError::write_vote(&e) })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;

        Ok(())
    }

    fn getStoreCF(&self) -> ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }

    fn getLogsCF(&self) -> ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn flush(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> Result<(), StorageIOError<NodeId>> {
        self.db.flush_wal(true).map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
        Ok(())
    }
}

impl RaftLogReader<RaftTypeConfigImpl> for RaftLogReaderStorageImpl {
    /// 和columnFamily logs交互,读取相应区间的
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> StorageResult<Vec<Entry<RaftTypeConfigImpl>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id2ByteVec(*x),
            std::ops::Bound::Excluded(x) => id2ByteVec(*x + 1),
            std::ops::Bound::Unbounded => id2ByteVec(0),
        };

        self.db.iterator_cf(&self.getLogsCF(), IteratorMode::From(&start, Direction::Forward)).map(
            |res| {
                let (id, val) = res.unwrap();

                let entry: StorageResult<Entry<_>> =
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO { source: StorageIOError::read_logs(&e) });
                let id = byteSlice2Id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));

                (id, entry)
            }).take_while(|(id, _)| range.contains(id)).map(|x| x.1).collect()
    }
}

impl RaftLogStorage<RaftTypeConfigImpl> for RaftLogReaderStorageImpl {
    type LogReader = Self;

    /// 和columnFamily logs交互,读取lastLogId
    async fn get_log_state(&mut self) -> StorageResult<LogState<RaftTypeConfigImpl>> {
        let mut lastLogId =
            self.db.iterator_cf(&self.getLogsCF(), IteratorMode::End).next().and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(serde_json::from_slice::<Entry<RaftTypeConfigImpl>>(&ent).ok()?.log_id)
            });

        let lastPurgedLogId = self.readLastPurgedLogId()?;

        if let None = lastLogId {
            lastLogId = lastPurgedLogId;
        }

        Ok(LogState {
            last_purged_log_id: lastPurgedLogId,
            last_log_id: lastLogId,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.saveVote(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.readVote()
    }

    async fn save_committed(&mut self, _committed: Option<LogId<NodeId>>) -> Result<(), StorageError<NodeId>> {
        self.saveCommitted(&_committed)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.readCommitted()?)
    }

    /// 和columnFamily logs交互,把entries记录下来
    #[tracing::instrument(level = "trace", skip_all)]
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<RaftTypeConfigImpl>) -> StorageResult<()>
    where
        I: IntoIterator<Item=Entry<RaftTypeConfigImpl>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let id = id2ByteVec(entry.log_id.index);
            println!("json: {}", serde_json::to_string(&entry).unwrap());
            self.db.put_cf(&self.getLogsCF(), id, serde_json::to_vec(&entry)
                .map_err(|e| StorageIOError::write_logs(&e))?)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    /// 和columnFamily logs交互,删掉相应的区间的
    #[tracing::instrument(level = "debug", skip(self))]
    async fn truncate(&mut self, logIdFromInclusive: LogId<NodeId>) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", logIdFromInclusive);

        let from = id2ByteVec(logIdFromInclusive.index);
        let to = id2ByteVec(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db.delete_range_cf(&self.getLogsCF(), &from, &to).map_err(|e| StorageIOError::write_logs(&e).into())
    }

    /// 和columnFamily logs交互,删掉相应的区间的
    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, lastPurgedLogId: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", lastPurgedLogId);

        self.saveLastPurgedLogId(lastPurgedLogId)?;

        let from = id2ByteVec(0);
        let to = id2ByteVec(lastPurgedLogId.index + 1);
        self.db.delete_range_cf(&self.getLogsCF(), &from, &to).map_err(|e| StorageIOError::write_logs(&e).into())
    }
}

pub fn id2ByteVec(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

pub fn byteSlice2Id(byteSlice: &[u8]) -> u64 {
    let (slice, _) = byteSlice.split_at(size_of::<u64>());
    u64::from_be_bytes(slice.try_into().unwrap())
}