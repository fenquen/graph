use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::Display;
use crate::graph_error::GraphError;
use crate::parser::{Element, LogicalOp, MathCalcOp, MathCmpOp, Op, SqlOp};
use crate::{global, meta, throw};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::ser::SerializeMap;
use crate::codec::{BinaryCodec, MyBytes};
use crate::global::{Byte};
use crate::meta::DataKey;

#[derive(Deserialize, Debug, Clone)]
pub enum GraphValue {
    /// 应对表的字段名 需要后续配合rowData来得到实际的
    Pending(String),
    String(String),
    Boolean(bool),
    Integer(i64),
    Decimal(f64),
    PointDesc(PointDesc),
}

/// type标识(u8) + 内容长度(u32,对应的是变长的 Pending String PoinstDesc) + 内容
impl BinaryCodec for GraphValue {
    type OutputType = GraphValue;

    fn decode(srcByteSlice: &mut MyBytes) -> Result<GraphValue> {
        // 读取type标识
        let typeTag = srcByteSlice.bytes.get_u8();

        match typeTag {
            GraphValue::PENDING | GraphValue::STRING | GraphValue::POINT_DESC => {
                let contentLen = srcByteSlice.bytes.get_u32() as usize;
                // let currentPos = srcByteSlice.position();
                // 不需要绝对的position 需要相对的 上边的绝对的currentPos用不到了
                let slice = &*srcByteSlice.bytes.slice(..contentLen);
                // 需要手动advence
                srcByteSlice.bytes.advance(contentLen);

                match typeTag {
                    GraphValue::PENDING => Ok(GraphValue::Pending(String::from_utf8_lossy(slice).to_string())),
                    GraphValue::STRING => Ok(GraphValue::String(String::from_utf8_lossy(slice).to_string())),
                    GraphValue::POINT_DESC => Ok(GraphValue::PointDesc(serde_json::from_slice(slice)?)),
                    _ => panic!("impossible")
                }
            }
            GraphValue::BOOLEAN => Ok(GraphValue::Boolean(srcByteSlice.bytes.get_u8() == 0)),
            GraphValue::INTEGER => Ok(GraphValue::Integer(srcByteSlice.bytes.get_i64())),
            GraphValue::DECIMAL => Ok(GraphValue::Decimal(srcByteSlice.bytes.get_f64())),
            _ => throw!(&format!("unknown type tag:{}",typeTag))
        }
    }

    fn encode(&self, destByteSlice: &mut BytesMut) -> Result<()> {
        match self {
            GraphValue::Pending(s) => {
                destByteSlice.put_u8(GraphValue::PENDING);
                destByteSlice.put_u32(s.len() as u32);
                destByteSlice.put_slice(s.as_bytes());
            }
            GraphValue::String(s) => {
                destByteSlice.put_u8(GraphValue::STRING);
                destByteSlice.put_u32(s.len() as u32);
                destByteSlice.put_slice(s.as_bytes());
            }
            GraphValue::Boolean(s) => {
                destByteSlice.put_u8(GraphValue::BOOLEAN);
                destByteSlice.put_u8(if *s { 1 } else { 0 });
            }
            GraphValue::Integer(s) => {
                destByteSlice.put_u8(GraphValue::INTEGER);
                destByteSlice.put_i64(*s);
            }
            GraphValue::Decimal(s) => {
                destByteSlice.put_u8(GraphValue::DECIMAL);
                destByteSlice.put_f64(*s);
            }
            GraphValue::PointDesc(pointDesc) => {
                destByteSlice.put_u8(GraphValue::POINT_DESC);
                let jsonString = serde_json::to_string(pointDesc)?;
                destByteSlice.put_u32(jsonString.as_bytes().len() as u32);
                destByteSlice.put_slice(jsonString.as_bytes());
            }
        }
        Ok(())
    }
}

impl Serialize for GraphValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> where S: Serializer {
        // 因为没有别的地方可以传递参数来标识了 不得已用threadLocal
        if global::UNTAGGED_ENUM_JSON.get() {
            match self {
                GraphValue::Pending(s) => s.serialize(serializer),
                GraphValue::String(s) => s.serialize(serializer),
                GraphValue::Boolean(s) => s.serialize(serializer),
                GraphValue::Integer(s) => s.serialize(serializer),
                GraphValue::Decimal(s) => s.serialize(serializer),
                GraphValue::PointDesc(s) => s.serialize(serializer)
            }
        } else {
            let mut serialMap = serializer.serialize_map(Some(1))?;

            match self {
                GraphValue::Pending(s) => {
                    serialMap.serialize_key("Pending")?;
                    serialMap.serialize_value(s)?;
                }
                GraphValue::String(s) => {
                    serialMap.serialize_key("String")?;
                    serialMap.serialize_value(s)?;
                }
                GraphValue::Boolean(s) => {
                    serialMap.serialize_key("Boolean")?;
                    serialMap.serialize_value(s)?;
                }
                GraphValue::Integer(s) => {
                    serialMap.serialize_key("Integer")?;
                    serialMap.serialize_value(s)?;
                }
                GraphValue::Decimal(s) => {
                    serialMap.serialize_key("Decimal")?;
                    serialMap.serialize_value(s)?;
                }
                GraphValue::PointDesc(s) => {
                    serialMap.serialize_key("PointDesc")?;
                    serialMap.serialize_value(s)?;
                }
            }

            serialMap.end()
        }
    }
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
            Element::PointDesc(pointDesc) => Ok(GraphValue::PointDesc(pointDesc.clone())),
            _ => throw!(&format!("element:{element:?} can not be transform to GraphValue")),
        }
    }
}

impl GraphValue {
    // 以下codec时候的type标识
    pub const PENDING: Byte = 0;
    pub const STRING: Byte = 1;
    pub const BOOLEAN: Byte = 2;
    pub const INTEGER: Byte = 3;
    pub const DECIMAL: Byte = 4;
    pub const POINT_DESC: Byte = 5;

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

/// relation data的用来描述两边的
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PointDesc {
    pub tableName: String,
    pub positions: Vec<DataKey>,
}

impl PointDesc {
    pub const SRC: &'static str = "src";
    pub const DEST: &'static str = "dest";
}