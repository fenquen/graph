use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, DBRawIteratorWithThreadMode,
              DBWithThreadMode, MultiThreaded, SnapshotWithThreadMode};
use serde_json::Value;
use crate::executor::CommandExecutor;
use crate::graph_value::GraphValue;


/// 到后台的sql可能是由多个小sql构成的 单个小select的sql对应个Vec<Value>
pub type SelectResultToFront = Vec<Vec<Value>>;

pub type DBRawIterator<'db> = DBRawIteratorWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;
pub type DBIterator<'db> = DBIteratorWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;

pub type ColumnFamily<'db> = Arc<BoundColumnFamily<'db>>;

pub type Snapshot<'db> = SnapshotWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;

pub type Byte = u8;

pub type TableId = u64;
pub type RowId = u64;

pub type KeyPrefix = Byte;
pub type DataKey = u64;
pub type KeyTag = Byte;

pub type TxId = u64;

pub type KV = (Vec<Byte>, Vec<Byte>);

pub type RowData = HashMap<String, GraphValue>;

pub trait ScanCommittedPreProcessor = FnMut(&ColumnFamily, DataKey) -> anyhow::Result<bool>;
pub trait ScanCommittedPostProcessor = FnMut(&ColumnFamily, DataKey, &RowData) -> anyhow::Result<bool>;
pub trait ScanUncommittedPreProcessor = FnMut(&TableMutations, DataKey) -> anyhow::Result<bool>;
pub trait ScanUncommittedPostProcessor = FnMut(&TableMutations, DataKey, &RowData) -> anyhow::Result<bool>;

pub type TableMutations = BTreeMap<Vec<Byte>, Vec<Byte>>;
