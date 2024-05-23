use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::mem;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use crate::graph_error::GraphError;
use crate::{byte_slice_to_u64, command_executor, file_goto_start, global, meta, suffix_plus_plus, throw, u64_to_byte_array_reference};
use anyhow::Result;
use tokio::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use lazy_static::lazy_static;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, DBCommon, DBRawIteratorWithThreadMode, IteratorMode, MultiThreaded, OptimisticTransactionDB, Options};
use tokio::sync::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use crate::config::CONFIG;
use crate::global::Byte;
use crate::graph_value::GraphValue;
use crate::parser::Command;
use crate::session::Session;
use crate::utils::TrickyContainer;

pub type RowId = u64;
pub type TableId = u64;

lazy_static! {
    pub static ref STORE: TrickyContainer<Store> = TrickyContainer::new();
    pub static ref TABLE_NAME_TABLE: DashMap<String, Table> = DashMap::new();
    pub static ref TABLE_ID_COUNTER: AtomicU64 = AtomicU64::default();
}

pub struct Store {
    pub meta: OptimisticTransactionDB,
    pub data: OptimisticTransactionDB,
}

pub type DataKey = u64;

// key的前缀 对普通的数据(key的前缀是KEY_PREFIX_DATA)来说是 prefix 4bit + rowId 60bit
pub const DATA_KEY_BYTE_LEN: usize = 8;

pub type KeyPrefix = Byte;

pub const KEY_PREFIX_BIT_LEN: usize = 4;
pub const KEY_PREFIX_MAX: KeyPrefix = (1 << KEY_PREFIX_BIT_LEN) - 1;

pub const KEY_PREFIX_DATA: KeyPrefix = 1;
pub const KEY_PREFIX_POINTER: KeyPrefix = 0;

pub const ROW_ID_BIT_LEN: usize = 64 - KEY_PREFIX_BIT_LEN;
pub const MAX_ROW_ID: u64 = (1 << ROW_ID_BIT_LEN) - 1;

pub const DATA_KEY_START_BINARY: &[Byte] = {
    let dataKeyStart = (KEY_PREFIX_DATA as u64) << ROW_ID_BIT_LEN;
    u64_to_byte_array_reference!(dataKeyStart)
};

// tag 用到POINTER前缀的key上的1Byte
pub type KeyTag = Byte;

pub const KEY_TAG_BYTE_LEN: usize = 1;
/// node 下游rel的tableId
pub const KEY_TAG_UPSTREAM_REL_ID: KeyTag = 0;
/// node 上游rel的tableId
pub const KEY_TAG_DOWNSTREAM_REL_ID: KeyTag = 1;
/// rel 的srcNode的tableId
pub const KEY_TAG_SRC_TABLE_ID: KeyTag = 2;
/// rel的destNode的tableId
pub const KEY_TAG_DEST_TABLE_ID: KeyTag = 3;
pub const KEY_TAG_KEY: KeyTag = 4;

pub const POINTER_KEY_BYTE_LEN: usize = {
    mem::size_of::<u64>() + // keyPrefix 4bit + rowId 60bit
        KEY_TAG_BYTE_LEN + DATA_KEY_BYTE_LEN + // table/relation的key
        KEY_TAG_BYTE_LEN + DATA_KEY_BYTE_LEN // 实际的data条目的key
};

pub const POINTER_LENADING_PART_BYTE_LEN: usize = POINTER_KEY_BYTE_LEN - DATA_KEY_BYTE_LEN;

#[macro_export]
macro_rules! key_prefix_add_row_id {
    ($keyPrefix: expr, $rowId: expr) => {
        (($keyPrefix as u64) << meta::ROW_ID_BIT_LEN) | (($rowId as u64) & meta::MAX_ROW_ID)
    };
}

#[macro_export]
macro_rules! extract_row_id_from_data_key {
    ($key: expr) => {
        (($key as u64) & meta::MAX_ROW_ID) as meta::RowId
    };
}

#[macro_export]
macro_rules! extract_prefix_from_key_1st_byte {
    ($byte: expr) => {
        ((($byte) >> meta::KEY_PREFIX_BIT_LEN) & meta::KEY_PREFIX_MAX) as meta::KeyPrefix
    };
}

#[macro_export]
macro_rules! extract_data_key_from_pointer_key_slice {
    ($pointerKeySlice: expr) => {
        {
            let slice = &$pointerKeySlice[(meta::POINTER_KEY_BYTE_LEN - meta::DATA_KEY_BYTE_LEN)..meta::POINTER_KEY_BYTE_LEN];
            byte_slice_to_u64!(slice) as meta::DataKey
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

    // 生成用来保存表文件和元数据的目录
    // meta的保存格式是 tableId->json
    let metaStore = {
        std::fs::create_dir_all(CONFIG.metaDir.as_str())?;

        let mut metaStoreOption = Options::default();
        metaStoreOption.set_keep_log_file_num(1);
        metaStoreOption.set_max_write_buffer_number(2);
        metaStoreOption.create_if_missing(true);

        let metaStore: OptimisticTransactionDB = OptimisticTransactionDB::open(&metaStoreOption, CONFIG.metaDir.as_str())?;

        let mut latestTableId = 0u64;

        let iterator = metaStore.iterator(IteratorMode::Start);
        for iterResult in iterator {
            let pair = iterResult?;

            let tableId = byte_slice_to_u64!(&*pair.0);
            let table: Table = serde_json::from_slice(&*pair.1)?;

            if tableId != table.tableId {
                throw!("table记录的key和table中的tableId不同");
            }

            tableNames.push(table.name.clone());

            TABLE_NAME_TABLE.insert(table.name.to_owned(), table);

            suffix_plus_plus!(latestTableId);
        }

        TABLE_ID_COUNTER.store(latestTableId, Ordering::Release);

        metaStore
    };

    let dataStore: OptimisticTransactionDB = {
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

        let dataStore: OptimisticTransactionDB = OptimisticTransactionDB::open_cf_descriptors(&dataStoreOption, CONFIG.dataDir.as_str(), columnFamilyDescVec)?;

        dataStore
    };

    // 遍历各个cf读取last的key 读取lastest的rowId
    for ref tableName in tableNames {
        let cf = dataStore.cf_handle(tableName.as_str()).unwrap();
        let mut iterator = dataStore.raw_iterator_cf(&cf);
        // 到last条目而不是末尾 不用去调用prev()
        iterator.seek_to_last();
        if let Some(key) = iterator.key() {
            let lastRowId = byte_slice_to_u64!(key);
            // println!("{tableName}, {}", lastRowId);
            TABLE_NAME_TABLE.get_mut(tableName.as_str()).unwrap().rowIdCounter.store(lastRowId + 1, Ordering::Release);
        }
    }

    STORE.set(Store {
        meta: metaStore,
        data: dataStore,
    });

    Ok(())
}

#[cfg(test)]
mod test {
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::runtime::Builder;
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
}
