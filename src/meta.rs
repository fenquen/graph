use std::fmt::{Display, Formatter, write};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use crate::graph_error::GraphError;
use crate::parser::{Element, LogicalOp, MathCmpOp, Op, SqlOp};
use crate::throw;
use anyhow::Result;

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

#[derive(Serialize, Deserialize, Debug)]
pub enum GraphValue {
    /// 应对表的字段名 需要后续配合rowData来得到实际的
    Pending(String),
    String(String),
    Boolean(bool),
    Integer(i64),
    Decimal(f64),
}

impl Display for GraphValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphValue::String(s) => write!(f, "String({})", s),
            GraphValue::Boolean(s) => write!(f, "Boolean({})", s),
            GraphValue::Integer(s) => write!(f, "Integer({})", s),
            GraphValue::Decimal(s) => write!(f, "Decimal({})", s),
            _ => write!(f, "unknown({:?})", self),
        }
    }
}

impl TryFrom<&Element> for GraphValue {
    type Error = GraphError;

    // 如何应对Element::TextLiteral 表的字段名是的
    fn try_from(element: &Element) -> Result<Self, Self::Error> {
        match element {
            Element::StringContent(s) => Ok(GraphValue::String(s.clone())),
            Element::Boolean(bool) => Ok(GraphValue::Boolean(*bool)),
            Element::IntegerLiteral(integer) => Ok(GraphValue::Integer(*integer)),
            Element::DecimalLiteral(decimal) => Ok(GraphValue::Decimal(*decimal)),
            Element::TextLiteral(columnName) => Ok(GraphValue::Pending(columnName.clone())),
            _ => throw!(&format!("element:{:?} can not be transform to GraphValue",element)),
        }
    }
}

impl GraphValue {
    pub fn boolValue(&self) -> Result<bool> {
        if let GraphValue::Boolean(bool) = self {
            Ok(*bool)
        } else {
            throw!(&format!("not boolean, is {:?}",self))
        }
    }

    /// 当前不支持 type的自动转换 两边的type要严格相同的
    pub fn calc(&self, op: Op, rightValues: &[GraphValue]) -> Result<GraphValue> {
        if let Op::SqlOp(SqlOp::In) = op {
            self.calcIn(rightValues)
        } else {
            self.calcOneToOne(op, &rightValues[0])
        }
    }

    pub fn calcOneToOne(&self, op: Op, rightValue: &GraphValue) -> Result<GraphValue> {
        match op {
            Op::MathCmpOp(mathCmpOp) => {
                match mathCmpOp {
                    _ => {}
                }
            }
            Op::MathCalcOp(matchCalcOp) => {
                match matchCalcOp {
                    _ => {}
                }
            }
            Op::LogicalOp(logicalOp) => {
                match logicalOp {
                    LogicalOp::Or => {}
                    LogicalOp::And => {}
                }
            }
            _ => throw!(&format!("unsupported op:{:?}",op)),
        }
        Ok(GraphValue::Boolean(false))
    }

    pub fn calcIn(&self, rightValues: &[GraphValue]) -> Result<GraphValue> {
        for rightValue in rightValues {
            let calcResult = self.calcOneToOne(Op::MathCmpOp(MathCmpOp::Equal), rightValue)?;
            if calcResult.boolValue()? == false {
                return Ok(GraphValue::Boolean(false));
            }
        }
        Ok(GraphValue::Boolean(true))
    }
}

#[cfg(test)]
mod test {
    use crate::meta::GraphValue;

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
}
