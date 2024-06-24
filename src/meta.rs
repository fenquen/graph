use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::mem;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use crate::graph_error::GraphError;
use crate::{byte_slice_to_u64, file_goto_start, global, meta, suffix_plus_plus, throw, types, u64ToByteArrRef};
use anyhow::Result;
use tokio::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use lazy_static::lazy_static;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, DB, DBCommon,
              DBRawIteratorWithThreadMode, IteratorMode, MultiThreaded, OptimisticTransactionDB, Options};
use std::sync::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use crate::config::CONFIG;
use crate::graph_value::GraphValue;
use crate::session::Session;
use crate::types::{Byte, DataKey, DBIterator, DBRawIterator, KeyPrefix, KeyTag, RowId, DBObjectId, TxId};
use crate::utils::TrickyContainer;

lazy_static! {
    pub static ref STORE: TrickyContainer<Store> = TrickyContainer::new();

    pub static ref NAME_DB_OBJ: DashMap<String, DBObject> = DashMap::new();
    // 如果是usize的可以使用::std::sync::atomic::ATOMIC_USIZE_INIT
    pub static ref DB_OBJECT_ID_COUNTER: AtomicU64 = AtomicU64::default();

    pub static ref TX_ID_COUNTER: AtomicU64 = AtomicU64::new(TX_ID_MIN);
    /// db启动的时候设置的原先已使用的最大的txId
    pub static ref TX_ID_START_UP: TrickyContainer<TxId> = TrickyContainer::new();
    pub static ref TX_UNDERGOING_COUNT:AtomicU64 = AtomicU64::default();

    //pub static ref TABLE_NAME_INDEX_NAMES: RwLock<HashMap<String, Vec<String>>> = Default::default();
}
pub struct Store {
    pub metaStore: DB,
    pub dataStore: DB,
}

pub const KEY_PREFIX_BIT_LEN: usize = 4;
pub const KEY_PREFIX_MAX: KeyPrefix = (1 << KEY_PREFIX_BIT_LEN) - 1;
pub const KEY_PREFIX_DATA: KeyPrefix = 0;
pub const KEY_PREFIX_POINTER: KeyPrefix = 1;
/// 应对的是data本身的 pointer体系的mvcc信息是在pointerKey末尾
pub const KEY_PREFIX_MVCC: KeyPrefix = 2;
pub const KEY_PPREFIX_ORIGIN_DATA_KEY: KeyPrefix = 3;

// ----------------------------------------------------------------------

pub const ROW_ID_BIT_LEN: usize = 64 - KEY_PREFIX_BIT_LEN;
pub const ROW_ID_MAX: RowId = (1 << ROW_ID_BIT_LEN) - 1;
pub const ROW_ID_MAX_AVAILABLE: RowId = ROW_ID_MAX - 1;
pub const ROW_ID_INVALID: RowId = 0;
pub const ROW_ID_MIN: RowId = 1;

// ----------------------------------------------------------------------

/// key的前缀 对普通的数据(key的前缀是KEY_PREFIX_DATA)来说是 prefix 4bit + rowId 60bit
pub const DATA_KEY_BYTE_LEN: usize = mem::size_of::<DataKey>();

// ----------------------------------------------------------------------

pub const DATA_KEY_PATTERN: &[Byte] = u64ToByteArrRef!((KEY_PREFIX_DATA as u64) << ROW_ID_BIT_LEN);
pub const POINTER_KEY_PATTERN: &[Byte] = u64ToByteArrRef!((KEY_PREFIX_POINTER as u64) << ROW_ID_BIT_LEN);
pub const MVCC_KEY_PATTERN: &[Byte] = u64ToByteArrRef!((KEY_PREFIX_MVCC as u64) << ROW_ID_BIT_LEN);

lazy_static! {
    pub static ref DATA_KEY_PATTERN_VEC: Vec<Byte> = DATA_KEY_PATTERN.to_vec();
    pub static ref POINTER_KEY_PATTERN_VEC :Vec<Byte> = POINTER_KEY_PATTERN.to_vec();
    pub static ref MVCC_KEY_PATTERN_VEC: Vec<Byte> = MVCC_KEY_PATTERN.to_vec();
}

// ----------------------------------------------------------------------

/// tag 用到POINTER前缀的key上的1Byte
pub const KEY_TAG_BYTE_LEN: usize = mem::size_of::<KeyTag>();

// ----------------------------------------------------------------------

pub const DB_OBJECT_ID_BYTE_LEN: usize = mem::size_of::<DBObjectId>();

// ----------------------------------------------------------------------

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

pub const POINTER_KEY_BYTE_LEN: usize = {
    DATA_KEY_BYTE_LEN + // keyPrefix 4bit + rowId 60bit
        KEY_TAG_BYTE_LEN + DB_OBJECT_ID_BYTE_LEN + // table/relation的id
        KEY_TAG_BYTE_LEN + DATA_KEY_BYTE_LEN + // 实际的data条目的key
        KEY_TAG_BYTE_LEN + TX_ID_BYTE_LEN // xmin和xmax 对应的tx
};

/// pointerKey的对端的dataKey前边的byte数量
pub const POINTER_KEY_TARGET_DATA_KEY_OFFSET: usize = POINTER_KEY_BYTE_LEN - TX_ID_BYTE_LEN - KEY_TAG_BYTE_LEN - DATA_KEY_BYTE_LEN;
pub const POINTER_KEY_MVCC_KEY_TAG_OFFSET: usize = POINTER_KEY_TARGET_DATA_KEY_OFFSET + DATA_KEY_BYTE_LEN;
pub const POINTER_KEY_TX_ID_OFFSET: usize = POINTER_KEY_MVCC_KEY_TAG_OFFSET + KEY_TAG_BYTE_LEN;

// ---------------------------------------------------------------------------------------

pub const TX_ID_BYTE_LEN: usize = mem::size_of::<TxId>();
pub const TX_ID_INVALID: TxId = 0;
pub const TX_ID_FROZEN: TxId = 2;
pub const TX_ID_MIN: TxId = 3;
pub const TX_ID_MAX: TxId = TxId::MAX;

pub const TX_UNDERGOING_MAX_COUNT: usize = 100000;

// ------------------------------------------------------------------------------------------

// KEY_PREFIX_MVCC + rowId + MVCC_KEY_TAG_XMIN + txId
pub const MVCC_KEY_TAG_XMIN: KeyTag = 0;
pub const MVCC_KEY_TAG_XMAX: KeyTag = 1;

pub const MVCC_KEY_BYTE_LEN: usize = {
    DATA_KEY_BYTE_LEN + KEY_TAG_BYTE_LEN + TX_ID_BYTE_LEN
};

// -----------------------------------------------------------------------------------------

/// 4bit + 60bit
pub const ORIGIN_DATA_KEY_KEY_BYTE_LEN: usize = mem::size_of::<DataKey>();
/// 是value啊不是像以往的key
pub const DATA_KEY_INVALID: DataKey = crate::keyPrefixAddRowId!(KEY_PREFIX_DATA, ROW_ID_INVALID);

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
macro_rules! keyPrefixAddRowId {
    ($keyPrefix: expr, $rowId: expr) => {
        (($keyPrefix as u64) << meta::ROW_ID_BIT_LEN) | (($rowId as u64) & meta::ROW_ID_MAX)
    };
}

#[macro_export]
macro_rules! extractRowIdFromDataKey {
    ($key: expr) => {
        (($key as u64) & meta::ROW_ID_MAX) as crate::types::RowId
    };
}

#[macro_export]
macro_rules! extractRowIdFromKeySlice {
    ($slice: expr) => {
        {
           let leadingU64 = byte_slice_to_u64!($slice);
           ((leadingU64) & meta::ROW_ID_MAX) as crate::types::RowId
        }
    };
}

#[macro_export]
macro_rules! extractPrefixFromKeySlice {
    ($slice: expr) => {
        ((($slice[0]) >> meta::KEY_PREFIX_BIT_LEN) & meta::KEY_PREFIX_MAX) as crate::types::KeyPrefix
    };
}

#[macro_export]
macro_rules! extractTargetDataKeyFromPointerKey {
    ($pointerKey: expr) => {
        {
            let slice = &$pointerKey[meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET..(meta::POINTER_KEY_TARGET_DATA_KEY_OFFSET + meta::DATA_KEY_BYTE_LEN)];
            byte_slice_to_u64!(slice) as crate::types::DataKey
        }
    };
}

/// txId 是在 mvccKey 末尾
#[macro_export]
macro_rules! extractTxIdFromMvccKey {
    ($mvccKey: expr) => {
        {
            let txIdSlice = &$mvccKey[(meta::MVCC_KEY_BYTE_LEN - meta::TX_ID_BYTE_LEN)..meta::MVCC_KEY_BYTE_LEN];
            byte_slice_to_u64!(txIdSlice) as crate::types::TxId
        }
    };
}

/// txId 是在 pointerkey 末尾
#[macro_export]
macro_rules! extractTxIdFromPointerKey {
    ($pointerKey: expr) => {
        {
            let slice = &$pointerKey[(meta::POINTER_KEY_BYTE_LEN - meta::TX_ID_BYTE_LEN)..meta::POINTER_KEY_BYTE_LEN];
            byte_slice_to_u64!(slice) as crate::types::TxId
        }
    };
}

#[macro_export]
macro_rules! extractMvccKeyTagFromPointerKey {
    ($pointerKey: expr) => {
        $pointerKey[meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET] as crate::types::KeyTag
    };
}

#[macro_export]
macro_rules! extractKeyTagFromMvccKey {
    ($mvccKey: expr) => {
        $mvccKey[meta::DATA_KEY_BYTE_LEN] as crate::types::KeyTag
    };
}

#[derive(Serialize, Deserialize)]
pub enum DBObject {
    Table(Table),
    Index(Index),
    Relation(Table),
}

impl DBObject {
    pub const TABLE: &'static str = "table";
    pub const INDEX: &'static str = "index";
    pub const RELATION: &'static str = "relation";

    pub fn asTable(&self) -> Result<&Table> {
        if let DBObject::Table(table) = self {
            Ok(table)
        } else {
            throw!(&format!("{} is not a table", self.getName()))
        }
    }

    pub fn asTableMut(&mut self) -> Result<&mut Table> {
        if let DBObject::Table(table) = self {
            Ok(table)
        } else {
            throw!(&format!("{} is not a table", self.getName()))
        }
    }

    pub fn asIndex(&self) -> Result<&Index> {
        if let DBObject::Index(index) = self {
            Ok(index)
        } else {
            throw!(&format!("{} is not a index", self.getName()))
        }
    }

    pub fn asRelation(&self) -> Result<&Table> {
        if let DBObject::Relation(table) = self {
            Ok(table)
        } else {
            throw!(&format!("{} is not a relation", self.getName()))
        }
    }

    pub fn getId(&self) -> DBObjectId {
        match self {
            DBObject::Table(table) => table.id,
            DBObject::Index(index) => index.id,
            DBObject::Relation(table) => table.id,
        }
    }

    pub fn getName(&self) -> String {
        match self {
            DBObject::Table(table) => table.name.clone(),
            DBObject::Index(index) => index.name.clone(),
            DBObject::Relation(table) => table.name.clone(),
        }
    }

    pub fn getRowIdCounter(&self) -> &AtomicU64 {
        match self {
            DBObject::Table(table) => &table.rowIdCounter,
            DBObject::Index(index) => &index.rowIdCounter,
            DBObject::Relation(table) => &table.rowIdCounter,
        }
    }
}

// todo alter table
// todo drop table
// todo 可以的话是不是记录下table的record的实际数量
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Table {
    /// start from 0
    pub id: DBObjectId,
    pub name: String,
    pub columns: Vec<Column>,
    #[serde(skip_serializing, skip_deserializing)]
    /// start from 1
    pub rowIdCounter: AtomicU64,
    pub createIfNotExist: bool,
    pub indexNames: Vec<String>,
}

#[derive(Debug, Deserialize, Clone, Serialize, Default)]
pub enum TableType {
    #[default]
    Table,
    Index,
    Relation,
}

impl FromStr for TableType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_lowercase().as_str() {
            "table" => Ok(TableType::Table),
            "index" => Ok(TableType::Index),
            "relation" => Ok(TableType::Relation),
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

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq, Default)]
pub enum ColumnType {
    #[default]
    String,
    Integer,
    Decimal,
}

impl ColumnType {
    pub fn compatible(&self, columnValue: &GraphValue) -> bool {
        match (self, columnValue) {
            (ColumnType::String, GraphValue::String(_)) => true,
            (ColumnType::Integer, GraphValue::Integer(_)) => true,
            (ColumnType::Decimal, GraphValue::Decimal(_)) => true,
            (_, GraphValue::Null) => true,
            _ => false
        }
    }
}

impl FromStr for ColumnType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_lowercase().as_str() {
            "string" => Ok(ColumnType::String),
            "integer" => Ok(ColumnType::Integer),
            "decimal" => Ok(ColumnType::Decimal),
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
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Index {
    pub id: DBObjectId,
    pub name: String,
    /// start from 1
    pub rowIdCounter: AtomicU64,
    pub createIfNotExist: bool,
    pub tableName: String,
    pub columnNames: Vec<String>,
}

pub fn init() -> Result<()> {
    // 用来后边dataStore时候对应各个的columnFamily
    let mut columnFamilyNames = Vec::new();

    // tx id 对应的column family
    columnFamilyNames.push(COLUMN_FAMILY_NAME_TX_ID.to_string());

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

            let dbObjectId = byte_slice_to_u64!(&*key);
            let dbObject: DBObject = serde_json::from_slice(&*value)?;

            if dbObjectId != dbObject.getId() {
                throw!("table记录的key和table中的tableId不同");
            }

            columnFamilyNames.push(dbObject.getName());

            NAME_DB_OBJ.insert(dbObject.getName(), dbObject);

            // key是以binary由大到小排序的 也便是table id由大到小排序
            latestTableId = dbObjectId;
        }

        DB_OBJECT_ID_COUNTER.store(latestTableId + 1, Ordering::Release);

        metaStore
    };

    let dataStore: DB = {
        let columnFamilyDescVec: Vec<ColumnFamilyDescriptor> =
            columnFamilyNames.iter().map(|columnFamilyName| {
                let mut columnFamilyOption = Options::default();
                columnFamilyOption.set_max_write_buffer_number(2);
                ColumnFamilyDescriptor::new(columnFamilyName, columnFamilyOption)
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
    for ref columnFamilyName in columnFamilyNames {
        let columnFamily = dataStore.cf_handle(columnFamilyName.as_str()).unwrap();
        let mut rawIterator: DBRawIterator = dataStore.raw_iterator_cf(&columnFamily);

        // 到last条目而不是末尾 不用去调用prev()
        rawIterator.seek_to_last();

        // todo latest的txId需要还原 完成
        match (rawIterator.key(), columnFamilyName.as_str()) {
            (Some(key), COLUMN_FAMILY_NAME_TX_ID) => {
                let lastTxId = byte_slice_to_u64!(key);

                TX_ID_START_UP.set(lastTxId);
                TX_ID_COUNTER.store(lastTxId + 1, Ordering::Release);
            }
            (Some(key), _) => {
                let lastRowId = extractRowIdFromKeySlice!(key);

                let dbObject = NAME_DB_OBJ.get(columnFamilyName.as_str()).unwrap();
                dbObject.getRowIdCounter().store(lastRowId + 1, Ordering::Release);
            }
            (None, COLUMN_FAMILY_NAME_TX_ID) => {
                TX_ID_START_UP.set(TX_ID_MIN - 1);
                TX_ID_COUNTER.store(TX_ID_MIN, Ordering::Release);
            }
            (None, _) => {
                let dbObject = NAME_DB_OBJ.get(columnFamilyName.as_str()).unwrap();
                dbObject.getRowIdCounter().store(ROW_ID_MIN, Ordering::Release);
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
