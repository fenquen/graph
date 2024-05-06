use std::fmt::{Display, Formatter, write};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use crate::graph_error::GraphError;
use crate::parser::{Element, LogicalOp, MathCalcOp, MathCmpOp, Op, SqlOp};
use crate::throw;
use anyhow::Result;
use crate::graph_value::GraphValue;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub type0: TableType,
    #[serde(skip_serializing, skip_deserializing)]
    pub dataFile: Option<File>,
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
