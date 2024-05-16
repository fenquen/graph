use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use crate::graph_error::GraphError;
use crate::{byte_slice_to_u64, command_executor, file_goto_start, global, suffix_plus_plus, throw};
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

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub type0: TableType,
    #[serde(skip_serializing, skip_deserializing)]
    pub rowIdCounter: AtomicU64,
    // start from 0
    pub tableId: TableId,
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
    // meta的保存格式是 tableName->json
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
    use crate::graph_value::GraphValue;

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
}
