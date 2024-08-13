use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use openraft::{Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageIOError, Vote};
use openraft::storage::{LogFlushed, RaftLogStorage};
use rocksdb::{DB, IteratorMode};
use crate::{meta, raft};
use crate::raft::{GraphRaftNodeId, GraphRaftTypeConfig, StorageResult};
use crate::types::DBRawIterator;

#[derive(Copy, Clone)]
pub struct GraphRaftLogReaderStorage {
    metaStore: &'static DB,
    dataStore: &'static DB,
    raftStore: &'static DB,
}

impl GraphRaftLogReaderStorage {
    const KEY_LAST_PURGED_LOG_ID: &'static [u8] = b"last_purged_log_id";
    const KEY_LAST_COMMTTED_LOG_ID: &'static [u8] = b"last_committed_log_id";
    const KEY_VOTE: &'static [u8] = b"vote";

    pub fn new() -> Self {
        GraphRaftLogReaderStorage {
            metaStore: &meta::STORE.metaStore,
            dataStore: &meta::STORE.dataStore,
            raftStore: &raft::RAFT_STORE,
        }
    }

    //---------------------------------------------------------------------------------

    /// 读取 last_purged_log_id
    fn readLastPurgedLogId(&self) -> StorageResult<Option<LogId<GraphRaftNodeId>>> {
        let lastPurgedLodId =
            match self.raftStore.get(Self::KEY_LAST_PURGED_LOG_ID).map_err(|e| StorageIOError::read(&e))? {
                Some(value) => serde_json::from_slice(&value).map_err(|e| StorageIOError::read(&e))?,
                None => return Ok(None)
            };

        Ok(Some(lastPurgedLodId))
    }

    // 写入 last_purged_log_id
    fn saveLastPurgedLogId(&self, logId: LogId<u64>) -> StorageResult<()> {
        let value = serde_json::to_vec(&logId).map_err(|e| StorageIOError::write(&e))?;
        self.raftStore.put(Self::KEY_LAST_PURGED_LOG_ID, value).map_err(|e| StorageIOError::write(&e))?;

        //self.flush(ErrorSubject::Store, ErrorVerb::Write)?;

        Ok(())
    }

    //---------------------------------------------------------------------------------

    /// 读取 last_committed_log_id
    fn readLastCommittedLogId(&self) -> StorageResult<Option<LogId<GraphRaftNodeId>>> {
        let lastCommittedLogId =
            match self.raftStore.get(Self::KEY_LAST_COMMTTED_LOG_ID).map_err(|e| StorageIOError::read(&e))? {
                Some(value) => serde_json::from_slice(&value).map_err(|e| StorageIOError::read(&e))?,
                None => return Ok(None)
            };

        Ok(Some(lastCommittedLogId))
    }

    /// 写入 last_committed_log_id
    fn saveLastCommittedLogId(&self, lastCommittedLogId: &Option<LogId<GraphRaftNodeId>>) -> StorageResult<()> {
        match lastCommittedLogId {
            Some(lastCommittedLogId) => {
                let value = serde_json::to_vec(lastCommittedLogId).map_err(|e| StorageIOError::write(&e))?;
                self.raftStore.put(Self::KEY_LAST_COMMTTED_LOG_ID, value).map_err(|e| StorageIOError::write(&e))?;
            }
            None => self.raftStore.delete(Self::KEY_LAST_COMMTTED_LOG_ID).map_err(|e| StorageIOError::write(&e))?,
        }

        //self.flush(ErrorSubject::Store, ErrorVerb::Write)?;

        Ok(())
    }

    //---------------------------------------------------------------------------------

    /// 读取 vote
    fn readVote(&self) -> StorageResult<Option<Vote<GraphRaftNodeId>>> {
        let vote =
            match self.raftStore.get(Self::KEY_VOTE).map_err(|e| StorageIOError::read_vote(&e))? {
                Some(value) => serde_json::from_slice(&value).map_err(|e| StorageIOError::read_vote(&e))?,
                None => return Ok(None)
            };

        Ok(Some(vote))
    }

    /// 写入 vote
    fn saveVote(&self, vote: &Vote<GraphRaftNodeId>) -> StorageResult<()> {
        let value = serde_json::to_vec(vote).map_err(|e| StorageIOError::write_vote(&e))?;
        self.raftStore.put(Self::KEY_VOTE, value).map_err(|e| StorageIOError::write_vote(&e))?;

        // self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;

        Ok(())
    }
}

impl RaftLogReader<GraphRaftTypeConfig> for GraphRaftLogReaderStorage {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> StorageResult<Vec<Entry<GraphRaftTypeConfig>>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let columnFamily =
            raft::getRaftColumnFamily(raft::COLUMN_FAMILY_NAME_LOG_ENTRIES).map_err(|e| StorageIOError::read_logs(e))?;

        let mut dbRawIterator: DBRawIterator = self.raftStore.raw_iterator_cf(&columnFamily);

        let start = match range.start_bound() {
            Bound::Excluded(start) => *start + 1,
            Bound::Included(start) => *start,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Excluded(end) => *end - 1,
            Bound::Included(end) => *end,
            Bound::Unbounded => u64::MAX,
        };

        dbRawIterator.seek(start.to_be_bytes());

        let mut entries = Vec::new();

        loop {
            let key = match dbRawIterator.key() {
                Some(key) => key,
                None => break,
            };

            if u64::from_be_bytes(key.try_into().unwrap()) > end {
                break;
            }

            let value = dbRawIterator.value().unwrap();
            let entry: Entry<GraphRaftTypeConfig> = serde_json::from_slice(value).map_err(|e| StorageIOError::read_logs(&e))?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<GraphRaftTypeConfig> for GraphRaftLogReaderStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<GraphRaftTypeConfig>> {
        let columnFamily =
            raft::getRaftColumnFamily(raft::COLUMN_FAMILY_NAME_LOG_ENTRIES).map_err(|e| StorageIOError::read(e))?;

        let mut dbRawIterator: DBRawIterator = self.raftStore.raw_iterator_cf(&columnFamily);
        dbRawIterator.seek_to_last();

        let mut lastLogId = match dbRawIterator.value() {
            Some(value) => {
                let lastEntry: Entry<GraphRaftTypeConfig> = serde_json::from_slice(value).map_err(|e| StorageIOError::read(&e))?;
                Some(lastEntry.log_id)
            }
            None => None
        };

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
        *self
    }

    async fn save_vote(&mut self, vote: &Vote<GraphRaftNodeId>) -> StorageResult<()> {
        self.saveVote(vote)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<GraphRaftNodeId>>> {
        self.readVote()
    }

    async fn save_committed(&mut self, committed: Option<LogId<GraphRaftNodeId>>) -> StorageResult<()> {
        self.saveLastCommittedLogId(&committed)
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<GraphRaftNodeId>>> {
        self.readLastCommittedLogId()
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<GraphRaftTypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item=Entry<GraphRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        for entry in entries {
            let columnFamily =
                raft::getRaftColumnFamily(raft::COLUMN_FAMILY_NAME_LOG_ENTRIES).map_err(|e| StorageIOError::write_log_entry(entry.log_id, e))?;

            self.raftStore.put_cf(
                &columnFamily,
                entry.log_id.index.to_be_bytes(),
                serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, logIdFromInclusive: LogId<GraphRaftNodeId>) -> StorageResult<()> {
        let columnFamily =
            raft::getRaftColumnFamily(raft::COLUMN_FAMILY_NAME_LOG_ENTRIES).map_err(|e| StorageIOError::write_logs(e))?;

        let from = logIdFromInclusive.index.to_be_bytes();
        let to = u64::MAX.to_be_bytes();

        self.raftStore.delete_range_cf(&columnFamily, from, to).map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    async fn purge(&mut self, lastPurgedLogId: LogId<GraphRaftNodeId>) -> StorageResult<()> {
        let columnFamily =
            raft::getRaftColumnFamily(raft::COLUMN_FAMILY_NAME_LOG_ENTRIES).map_err(|e| StorageIOError::write_logs(e))?;

        let from = 0u64.to_be_bytes();
        let to = lastPurgedLogId.index.to_be_bytes();

        self.raftStore.delete_range_cf(&columnFamily, from, to).map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }
}