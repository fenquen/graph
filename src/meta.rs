use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use crate::graph_error::GraphError;
use crate::{command_executor, file_goto_start, global, throw};
use anyhow::Result;
use tokio::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use crate::config::CONFIG;
use crate::graph_value::GraphValue;
use crate::parser::Command;
use crate::session::Session;

pub const TABLE_RECORD_FILE_NAME: &str = "table_record";
pub const WAL_FILE_NAME: &str = "wal";

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub type0: TableType,
    #[serde(skip_serializing, skip_deserializing)]
    pub dataFile: Option<File>,
    #[serde(skip_serializing, skip_deserializing)]
    pub restore: bool,
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

pub async fn init() -> Result<()> {
    // 生成用来保存表文件和元数据的目录
    fs::create_dir_all(CONFIG.dataDir.as_str()).await?;
    fs::create_dir_all(CONFIG.metaDir.as_str()).await?;

    let metaDirPath: &Path = CONFIG.metaDir.as_ref();

    // table_record
    let mut tableRecordFile = OpenOptions::new().write(true).read(true).create(true).open(metaDirPath.join(TABLE_RECORD_FILE_NAME)).await?;
    // 上边不能使用append(true),不然的话不论如何seek都只会到末尾append
    file_goto_start!(tableRecordFile);

    // wal
    let mut walFile = OpenOptions::new().write(true).read(true).create(true).open(metaDirPath.join(WAL_FILE_NAME)).await?;
    file_goto_start!(walFile);

    // 还原
    restoreDB(&mut tableRecordFile, &mut walFile).await?;

    global::TABLE_RECORD_FILE.store(Arc::new(Some(RwLock::new(tableRecordFile))));
    global::WAL_FILE.store(Arc::new(Some(RwLock::new(walFile))));

    Ok(())
}

async fn restoreDB(tableRecordFile: &mut File, walFile: &mut File) -> Result<()> {
    // 还原table
    {
        let bufReader = BufReader::new(tableRecordFile);
        let mut lines = bufReader.lines();

        let mut session = Session::new();

        while let Some(line) = lines.next_line().await? {
            let mut table: Table = serde_json::from_str(&line)?;
            table.restore = true;
            session.executeCommands(&vec![Command::CreateTable(table)]).await?;
        }
    }

    // 还原 txId
    {
        if walFile.seek(SeekFrom::End(0)).await? > 0 {
            walFile.seek(SeekFrom::End(global::TX_ID_LEN as i64 * -1)).await?;
            let lastTxId = walFile.read_u64().await?;
            global::TX_ID_COUNTER.store(lastTxId, Ordering::SeqCst);

            println!("lastTxId:{}", lastTxId);
        }
    }

    Ok(())
}
