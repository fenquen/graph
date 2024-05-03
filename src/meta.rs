use std::fmt::{Display, Formatter};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use crate::graph_error::GraphError;
use crate::throw;

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
    TABLE,
    RELATION,
    Unknown,
}

impl Default for TableType {
    fn default() -> Self {
        TableType::Unknown
    }
}

impl From<&str> for TableType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "TABLE" => TableType::TABLE,
            "RELATION" => TableType::RELATION,
            _ => TableType::Unknown
        }
    }
}

impl FromStr for TableType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_uppercase().as_str() {
            "TABLE" => Ok(TableType::TABLE),
            "RELATION" => Ok(TableType::RELATION),
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
    STRING,
    INTEGER,
    DECIMAL,
    UNKNOWN,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::UNKNOWN
    }
}

impl ColumnType {
    pub fn compatible(&self, columnValue: &Value) -> bool {
        match self {
            ColumnType::STRING => {
                if let Value::STRING(_) = columnValue {
                    return true;
                }
            }
            ColumnType::INTEGER => {
                if let Value::INTEGER(_) = columnValue {
                    return true;
                }
            }
            ColumnType::DECIMAL => {
                if let Value::DECIMAL(_) = columnValue {
                    return true;
                }
            }
            _ => {}
        }

        false
    }
}

impl From<&str> for ColumnType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "STRING" => ColumnType::STRING,
            "INTEGER" => ColumnType::INTEGER,
            "DECIMAL" => ColumnType::DECIMAL,
            _ => ColumnType::UNKNOWN
        }
    }
}

impl FromStr for ColumnType {
    type Err = GraphError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str.to_uppercase().as_str() {
            "STRING" => Ok(ColumnType::STRING),
            "INTEGER" => Ok(ColumnType::INTEGER),
            "DECIMAL" => Ok(ColumnType::DECIMAL),
            _ => throw!(&format!("unknown type:{}", str))
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::STRING => write!(f, "STRING"),
            ColumnType::INTEGER => write!(f, "INTEGER"),
            ColumnType::DECIMAL => write!(f, "DECIMAL"),
            _ => write!(f, "UNKNOWN"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Value {
    STRING(String),
    INTEGER(i64),
    DECIMAL(f64),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::STRING(s) => write!(f, "STRING({})", s),
            Value::INTEGER(s) => write!(f, "INTEGER({})", s),
            Value::DECIMAL(s) => write!(f, "DECIMAL({})", s),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::meta::Value;

    #[test]
    pub fn testSerialEnum() {
        let a = Value::STRING("s".to_string());
        println!("{}", serde_json::to_string(&a).unwrap());
    }

    #[test]
    pub fn testDeserialEnum() {
        let columnValue: Value = serde_json::from_str("{\"STRING\":\"s\"}").unwrap();
        if let Value::STRING(s) = columnValue {
            println!("{}", s);
        }
    }
}
