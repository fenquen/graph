use std::cell::Cell;
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::mem;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use crate::graph_error::GraphError;
use crate::{byte_slice_to_u64, command_executor, file_goto_start, global, meta, suffix_plus_plus, throw, types, u64_to_byte_array_reference};
use anyhow::Result;
use tokio::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use lazy_static::lazy_static;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, DB, DBCommon,
              DBRawIteratorWithThreadMode, IteratorMode, MultiThreaded, OptimisticTransactionDB, Options};
use tokio::sync::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use crate::config::CONFIG;
use crate::graph_value::GraphValue;
use crate::parser::Command;
use crate::session::Session;
use crate::types::{Byte, DataKey, DBIterator, DBRawIterator, KeyPrefix, KeyTag, TableId, TxId};
use crate::utils::TrickyContainer;

lazy_static! {
    pub static ref STORE: TrickyContainer<Store> = TrickyContainer::new();
    pub static ref TABLE_NAME_TABLE: DashMap<String, Table> = DashMap::new();
    pub static ref TABLE_ID_COUNTER: AtomicU64 = AtomicU64::default();
    pub static ref TX_ID_COUNTER: AtomicU64 = AtomicU64::new(TX_ID_MIN);
    pub static ref TX_ID_START_UP: TrickyContainer<TxId> = TrickyContainer::new();
}

pub struct Store {
    pub metaStore: DB,
    pub dataStore: DB,
}

pub const KEY_PREFIX_BIT_LEN: usize = 4;
pub const KEY_PREFIX_MAX: KeyPrefix = (1 << KEY_PREFIX_BIT_LEN) - 1;
pub const KEY_PREFIX_DATA: KeyPrefix = 0;
pub const KEY_PREFIX_POINTER: KeyPrefix = 1;
pub const KEY_PREFIX_MVCC: KeyPrefix = 2;

pub const ROW_ID_BIT_LEN: usize = 64 - KEY_PREFIX_BIT_LEN;
pub const MAX_ROW_ID: u64 = (1 << ROW_ID_BIT_LEN) - 1;

// key的前缀 对普通的数据(key的前缀是KEY_PREFIX_DATA)来说是 prefix 4bit + rowId 60bit
pub const DATA_KEY_BYTE_LEN: usize = mem::size_of::<DataKey>();
pub const DATA_KEY_PATTERN: &[Byte] = u64_to_byte_array_reference!((KEY_PREFIX_DATA as u64) << ROW_ID_BIT_LEN);
pub const POINTER_KEY_PATTERN: &[Byte] = u64_to_byte_array_reference!((KEY_PREFIX_POINTER as u64) << ROW_ID_BIT_LEN);
pub const MVCC_KEY_PATTERN: &[Byte] = u64_to_byte_array_reference!((KEY_PREFIX_MVCC as u64) << ROW_ID_BIT_LEN);

lazy_static! {
    pub static ref DATA_KEY_PATTERN_VEC: Vec<Byte> = DATA_KEY_PATTERN.to_vec();
    pub static ref POINTER_KEY_PATTERN_VEC :Vec<Byte> = POINTER_KEY_PATTERN.to_vec();
    pub static ref MVCC_KEY_PATTERN_VEC: Vec<Byte> = MVCC_KEY_PATTERN.to_vec();
}

// tag 用到POINTER前缀的key上的1Byte
pub const KEY_TAG_BYTE_LEN: usize = 1;

/// node 下游rel的tableId
pub const POINTER_KEY_TAG_UPSTREAM_REL_ID: KeyTag = 0;
/// node 上游rel的tableId
pub const POINTER_KEY_TAG_DOWNSTREAM_REL_ID: KeyTag = 1;
/// rel 的srcNode的tableId
pub const POINTER_KEY_TAG_SRC_TABLE_ID: KeyTag = 2;
/// rel的destNode的tableId
pub const POINTER_KEY_TAG_DEST_TABLE_ID: KeyTag = 3;
/// 后边实际的table/rel上的dataKey
pub const POINTER_KEY_TAG_DATA_KEY: KeyTag = 4;
// pub const POINTER_KEY_TAG_XMIN: KeyTag = 5;
// pub const POINTER_KEY_TAG_XMAX: KeyTag = 7;

pub const POINTER_KEY_BYTE_LEN: usize = {
    DATA_KEY_BYTE_LEN + // keyPrefix 4bit + rowId 60bit
        KEY_TAG_BYTE_LEN + DATA_KEY_BYTE_LEN + // table/relation的id
        KEY_TAG_BYTE_LEN + DATA_KEY_BYTE_LEN + // 实际的data条目的key
        KEY_TAG_BYTE_LEN + TX_ID_BYTE_LEN // xmin和xmax 对应的tx
};

/// pointerKey的对端的dataKey前边的byte数量
pub const POINTER_KEY_TARGET_DATA_KEY_OFFSET: usize = POINTER_KEY_BYTE_LEN - KEY_TAG_BYTE_LEN - DATA_KEY_BYTE_LEN - TX_ID_BYTE_LEN;
pub const POINTER_KEY_MVCC_KEY_TAG_OFFSET: usize = POINTER_KEY_TARGET_DATA_KEY_OFFSET + DATA_KEY_BYTE_LEN;

// 写到dataKey
// add put 添加新的
// update  删掉old 添加作废的old 添加新的 -> 修改old 添加新的
// delete 删掉old 添加作废的old -> 修改old

// 写到 mvcc key
// add 写dataKey 写mvcc_xmin 写mvcc_xmax
// update 删掉
pub const TX_ID_BYTE_LEN: usize = mem::size_of::<TxId>();
pub const TX_ID_INVALID: TxId = 0;
pub const TX_ID_FROZEN: TxId = 2;
pub const TX_ID_MIN: TxId = 3;
pub const TX_ID_MAX: TxId = TxId::MAX;

pub const TX_CONCURRENCY_MAX: usize = 100000;

// KEY_PREFIX_MVCC + rowId + MVCC_KEY_TAG_XMIN + txId
pub const MVCC_KEY_TAG_XMIN: KeyTag = 0;
pub const MVCC_KEY_TAG_XMAX: KeyTag = 1;

pub const MVCC_KEY_BYTE_LEN: usize = {
    DATA_KEY_BYTE_LEN + KEY_TAG_BYTE_LEN + TX_ID_BYTE_LEN
};

/// 用来保存txId的colFamily的name
pub const COLUMN_FAMILY_NAME_TX_ID: &str = "tx_id";

pub fn isVisible(currentTxId: TxId, xmin: TxId, xmax: TxId) -> bool {
    // invisible
    if currentTxId >= xmax {
        if (xmax == TX_ID_INVALID) == false {
            return false;
        }
    }

    // invisible
    if xmin > currentTxId {
        return false;
    }

    true
}

#[macro_export]
macro_rules! key_prefix_add_row_id {
    ($keyPrefix: expr, $rowId: expr) => {
        (($keyPrefix as u64) << meta::ROW_ID_BIT_LEN) | (($rowId as u64) & meta::MAX_ROW_ID)
    };
}

#[macro_export]
macro_rules! extract_row_id_from_data_key {
    ($key: expr) => {
        (($key as u64) & meta::MAX_ROW_ID) as types::RowId
    };
}

#[macro_export]
macro_rules! extract_row_id_from_key_slice {
    ($slice: expr) => {
        {
           let leadingU64 = byte_slice_to_u64!($slice);
           ((leadingU64) & meta::MAX_ROW_ID) as types::RowId
        }
    };
}

#[macro_export]
macro_rules! extract_prefix_from_key_slice {
    ($slice: expr) => {
        ((($slice[0]) >> meta::KEY_PREFIX_BIT_LEN) & meta::KEY_PREFIX_MAX) as types::KeyPrefix
    };
}

#[macro_export]
macro_rules! extract_target_data_key_from_pointer_key {
    ($pointerKey: expr) => {
        {
            let slice = &$pointerKey[meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET..(meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET + meta::DATA_KEY_BYTE_LEN)];
            byte_slice_to_u64!(slice) as types::DataKey
        }
    };
}

/// txId 是在 mvccKey 末尾
#[macro_export]
macro_rules! extract_tx_id_from_mvcc_key {
    ($mvccKey: expr) => {
        {
            let txIdSlice = &$mvccKey[(meta::MVCC_KEY_BYTE_LEN - meta::TX_ID_BYTE_LEN)..meta::MVCC_KEY_BYTE_LEN];
            byte_slice_to_u64!(txIdSlice) as types::TxId
        }
    };
}

/// txId 是在 pointerkey 末尾
#[macro_export]
macro_rules! extract_tx_id_from_pointer_key {
    ($pointerKey: expr) => {
        {
            let slice = &$pointerKey[(meta::POINTER_KEY_BYTE_LEN - meta::TX_ID_BYTE_LEN)..meta::POINTER_KEY_BYTE_LEN];
            byte_slice_to_u64!(slice) as types::TxId
        }
    };
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub type0: TableType,
    #[serde(skip_serializing, skip_deserializing)]
    pub rowIdCounter: AtomicU64,
    // start from 0
    pub tableId: TableId,
    pub createIfNotExist: bool,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub enum TableType {
    Table,
    Relation,
    Unknown,
}

impl Default for TableType {
    fn default() -> Self {
        TableType::Unknown
    }
}

impl FromStr for TableType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_uppercase().as_str() {
            "TABLE" => Ok(TableType::Table),
            "RELATION" => Ok(TableType::Relation),
            _ => throw!(&format!("unknown type:{}", str)),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Serialize, Default)]
pub struct Column {
    pub name: String,
    pub type0: ColumnType,
    pub nullable: bool,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.type0 == other.type0
    }
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub enum ColumnType {
    String,
    Integer,
    Decimal,
    /// 内部使用的 当创建relation时候用到 用来给realtion添加2个额外的字段 src dest
    PointDesc,
    Unknown,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::Unknown
    }
}

impl ColumnType {
    pub fn compatible(&self, columnValue: &GraphValue) -> bool {
        match (self, columnValue) {
            (ColumnType::String, GraphValue::String(_)) => true,
            (ColumnType::Integer, GraphValue::Integer(_)) => true,
            (ColumnType::Decimal, GraphValue::Decimal(_)) => true,
            (ColumnType::PointDesc, GraphValue::PointDesc(_)) => true,
            (_, GraphValue::Null) => true,
            _ => false
        }
    }
}

impl From<&str> for ColumnType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "STRING" => ColumnType::String,
            "INTEGER" => ColumnType::Integer,
            "DECIMAL" => ColumnType::Decimal,
            _ => ColumnType::Unknown
        }
    }
}

impl FromStr for ColumnType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_uppercase().as_str() {
            "STRING" => Ok(ColumnType::String),
            "INTEGER" => Ok(ColumnType::Integer),
            "DECIMAL" => Ok(ColumnType::Decimal),
            _ => throw!(&format!("unknown type:{}", str))
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::String => write!(f, "STRING"),
            ColumnType::Integer => write!(f, "INTEGER"),
            ColumnType::Decimal => write!(f, "DECIMAL"),
            _ => write!(f, "UNKNOWN"),
        }
    }
}

pub fn init() -> Result<()> {
    // 用来后边dataStore时候对应各个的columnFamily
    let mut tableNames = Vec::new();

    // tx id 对应的column family
    tableNames.push(COLUMN_FAMILY_NAME_TX_ID.to_string());

    // 生成用来保存表文件和元数据的目录
    // meta的保存格式是 tableId->json
    let metaStore = {
        std::fs::create_dir_all(CONFIG.metaDir.as_str())?;

        let mut metaStoreOption = Options::default();
        metaStoreOption.set_keep_log_file_num(1);
        metaStoreOption.set_max_write_buffer_number(2);
        metaStoreOption.create_if_missing(true);

        let metaStore = DB::open(&metaStoreOption, CONFIG.metaDir.as_str())?;

        // todo tableId的计数不对 要以当前max的table id不能以表的数量  完成
        let mut latestTableId = 0u64;

        let iterator: DBIterator = metaStore.iterator(IteratorMode::Start);
        for iterResult in iterator {
            let (key, value) = iterResult?;

            let tableId = byte_slice_to_u64!(&*key);
            let table: Table = serde_json::from_slice(&*value)?;

            if tableId != table.tableId {
                throw!("table记录的key和table中的tableId不同");
            }

            tableNames.push(table.name.clone());

            TABLE_NAME_TABLE.insert(table.name.to_owned(), table);

            // key是以binary由大到小排序的 也便是table id由大到小排序
            latestTableId = tableId;
        }

        TABLE_ID_COUNTER.store(latestTableId + 1, Ordering::Release);

        metaStore
    };

    let dataStore: DB = {
        let columnFamilyDescVec: Vec<ColumnFamilyDescriptor> =
            tableNames.iter().map(|tableName| {
                let mut columnFamilyOption = Options::default();
                columnFamilyOption.set_max_write_buffer_number(2);
                ColumnFamilyDescriptor::new(tableName, columnFamilyOption)
            }).collect::<Vec<ColumnFamilyDescriptor>>();

        std::fs::create_dir_all(CONFIG.dataDir.as_str())?;

        let mut dataStoreOption = Options::default();
        // 默认日志保留的数量1000 太多
        dataStoreOption.set_keep_log_file_num(1);
        dataStoreOption.set_max_write_buffer_number(2);
        dataStoreOption.create_missing_column_families(true);
        dataStoreOption.create_if_missing(true);

        DB::open_cf_descriptors(&dataStoreOption, CONFIG.dataDir.as_str(), columnFamilyDescVec)?
    };

    // 遍历各个cf读取last的key 读取lastest的rowId
    for ref tableName in tableNames {
        let cf = dataStore.cf_handle(tableName.as_str()).unwrap();
        let mut rawIterator: DBRawIterator = dataStore.raw_iterator_cf(&cf);

        // 到last条目而不是末尾 不用去调用prev()
        rawIterator.seek_to_last();

        if let Some(key) = rawIterator.key() {
            // todo latest的txId需要还原
            // 应对的记录txId的column family
            // 读取last的key对应的latest的tx id
            if tableName == COLUMN_FAMILY_NAME_TX_ID {
                let lastTxId = byte_slice_to_u64!(key);

                TX_ID_START_UP.set(lastTxId);
                TX_ID_COUNTER.store(lastTxId + 1, Ordering::Release);
            } else {
                // 严格意义上应该只要dataKey的部分
                // 不过现在这样的话也是可以的,因为不管是什么key它的后60bit都是rowId
                let lastRowId = byte_slice_to_u64!(key);
                TABLE_NAME_TABLE.get_mut(tableName.as_str()).unwrap().rowIdCounter.store(lastRowId + 1, Ordering::Release);
            }
        }
    }

    STORE.set(Store {
        metaStore,
        dataStore,
    });


    Ok(())
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::time::Duration;
    use rocksdb::{DB, OptimisticTransactionDB, Options, TransactionDB, WriteBatchWithTransaction};
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::runtime::Builder;
    use crate::config::CONFIG;
    use crate::graph_value::GraphValue;
    use crate::meta;
    use crate::session::Session;

    #[test]
    pub fn testSerialEnum() {
        let a = GraphValue::String("s".to_string());
        println!("{}", serde_json::to_string(&a).unwrap());
    }

    #[test]
    pub fn testDeserialEnum() {
        let columnValue: GraphValue = serde_json::from_str("{\"STRING\":\"s\"}").unwrap();
        if let GraphValue::String(s) = columnValue {
            println!("{}", s);
        }
    }

    #[test]
    pub fn testStringEqual() {
        let a = "a".to_string();
        let b = "a".to_string();
        println!("{}", a == b);
    }

    #[test]
    pub fn manauallyExecuteSql() -> anyhow::Result<()> {
        meta::init()?;

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let sqlRecord = OpenOptions::new().read(true).open("sql.txt").await?;
            let bufReader = BufReader::new(sqlRecord);
            let mut sqls = bufReader.lines();

            let mut session = Session::new();

            while let Some(sql) = sqls.next_line().await? {
                if sql.starts_with("--") {
                    continue;
                }

                session.executeSql(sql.as_str())?;
            }

            anyhow::Result::<()>::Ok(())
        })?;

        Ok(())
    }

    #[test]
    pub fn testRocksDB() {
        let transactionDB: TransactionDB = TransactionDB::open_default("test").unwrap();

        let tx0 = transactionDB.transaction();
        tx0.put(&[0], &[1]).unwrap();

        let tx1 = transactionDB.transaction();
        tx1.put(&[0], &[2]).unwrap();

        tx0.commit().unwrap();

        // 两个tx产生了交集 key conflict 报错resource busy
        tx1.commit().unwrap();
    }

    #[test]
    pub fn testWriteBatch() {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open_default("test").unwrap();

        let mut writeBatchWithTx0 = WriteBatchWithTransaction::<false>::default();
        writeBatchWithTx0.put(&[0], &[1]);

        let mut writeBatchWithTx1 = WriteBatchWithTransaction::<false>::default();
        writeBatchWithTx1.put(&[0], &[1]);

        db.write(writeBatchWithTx0).unwrap();

        db.write(writeBatchWithTx1).unwrap();
    }
}
