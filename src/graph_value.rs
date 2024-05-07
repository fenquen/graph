use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use strum_macros::Display;
use crate::graph_error::GraphError;
use crate::parser::{Element, LogicalOp, MathCalcOp, MathCmpOp, Op, SqlOp};
use crate::throw;
use anyhow::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GraphValue {
    /// 应对表的字段名 需要后续配合rowData来得到实际的
    Pending(String),
    String(String),
    Boolean(bool),
    Integer(i64),
    Decimal(f64),
    PointDesc(PointDesc),
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
            _ => throw!(&format!("element:{element:?} can not be transform to GraphValue")),
        }
    }
}

impl GraphValue {
    pub fn boolValue(&self) -> Result<bool> {
        if let GraphValue::Boolean(bool) = self {
            Ok(*bool)
        } else {
            throw!(&format!("not boolean, is {self:?}"))
        }
    }

    pub fn asPointDesc(&self) -> Result<&PointDesc> {
        if let GraphValue::PointDesc(pointDesc) = self {
            Ok(pointDesc)
        } else {
            throw!(&format!("not PointDEsc, is {self:?}"))
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

    /// 目前对不兼容的type之间的大小比较返回false
    pub fn calcOneToOne(&self, op: Op, rightValue: &GraphValue) -> Result<GraphValue> {
        match op {
            Op::MathCmpOp(mathCmpOp) => {
                match mathCmpOp {
                    MathCmpOp::LessEqual => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s <= s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b <= b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer <= integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 <= &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float <= float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 <= &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                    MathCmpOp::Equal => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s == s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b == b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer == integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 == &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float == float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 == &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                    MathCmpOp::LessThan => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s < s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b < b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer < integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 < &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float < float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 < &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                    MathCmpOp::GreaterThan => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s > s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b > b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer > integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 > &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float > float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 > &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                    MathCmpOp::GreaterEqual => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s >= s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b >= b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer >= integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 >= &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float >= float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 >= &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                    MathCmpOp::NotEqual => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::Boolean(s != s0)),
                            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Ok(GraphValue::Boolean(b != b0)),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Boolean(integer != integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Boolean(float64 != &(*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Boolean(float != float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Boolean(float64 != &(*integer as f64))),
                            _ => Ok(GraphValue::Boolean(false)),
                        }
                    }
                }
            }
            Op::MathCalcOp(matchCalcOp) => {
                match matchCalcOp {
                    MathCalcOp::Plus => {
                        match (self, rightValue) {
                            (GraphValue::String(s), GraphValue::String(s0)) => Ok(GraphValue::String(format!("{s}{s0}"))),
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer + integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 + (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float + float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 + (*integer as f64))),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                    MathCalcOp::Divide => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer / integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 / (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float / float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 / (*integer as f64))),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                    MathCalcOp::Multiply => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer * integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 * (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float * float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 * (*integer as f64))),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                    MathCalcOp::Minus => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer - integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 - (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float - float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 - (*integer as f64))),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                }
            }
            Op::LogicalOp(logicalOp) => {
                match logicalOp {
                    LogicalOp::Or => {
                        match (self, rightValue) {
                            (GraphValue::Boolean(bool), GraphValue::Boolean(bool0)) => Ok(GraphValue::Boolean(bool | bool0)),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                    LogicalOp::And => {
                        match (self, rightValue) {
                            (GraphValue::Boolean(bool), GraphValue::Boolean(bool0)) => Ok(GraphValue::Boolean(bool & bool0)),
                            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
                        }
                    }
                }
            }
            _ => throw!(&format!("can not use {op:?}, between {self:?} , {rightValue:?}")),
        }
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PointDesc {
    pub tableName: String,
    pub positions: Vec<u64>,
}

impl PointDesc {
    pub const SRC: &'static str = "src";
    pub const DEST: &'static str = "dest";
}