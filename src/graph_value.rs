use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::Display;
use crate::graph_error::GraphError;
use crate::{global, meta, throw, throwFormat, utils};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::ser::SerializeMap;
use serde_json::Value;
use crate::codec::{BinaryCodec, MyBytes};
use crate::parser::element::Element;
use crate::parser::op::{determineLikePattern, LikePattern, LogicalOp, MathCalcOp, MathCmpOp, Op, SqlOp};
use crate::types::{Byte, DataKey};

#[derive(Deserialize, Debug, Clone)]
pub enum GraphValue {
    /// 应对表的字段名 需要后续配合rowData来得到实际的
    Pending(String),

    String(String),
    Boolean(bool),
    Integer(i64),
    Decimal(f64),
    Null,

    IndexUseful { columnName: String, op: Op, values: Vec<GraphValue> },
    IndexUseless,
}

/// type标识(u8) + 内容长度(u32,对应的是变长的 Pending String PoinstDesc) + 内容
impl BinaryCodec for GraphValue {
    type OutputType = GraphValue;

    fn decode(srcByteSlice: &mut MyBytes) -> Result<GraphValue> {
        // 读取type标识
        let typeTag = srcByteSlice.bytes.get_u8();

        match typeTag {
            GraphValue::PENDING | GraphValue::STRING => {
                let contentLen = srcByteSlice.bytes.get_u32() as usize;
                // let currentPos = srcByteSlice.position();
                // 不需要绝对的position 需要相对的 上边的绝对的currentPos用不到了
                let slice = &*srcByteSlice.bytes.slice(..contentLen);
                // 需要手动advence
                srcByteSlice.bytes.advance(contentLen);

                match typeTag {
                    GraphValue::PENDING => Ok(GraphValue::Pending(String::from_utf8_lossy(slice).to_string())),
                    GraphValue::STRING => Ok(GraphValue::String(String::from_utf8_lossy(slice).to_string())),
                    _ => panic!("impossible")
                }
            }
            GraphValue::BOOLEAN => Ok(GraphValue::Boolean(srcByteSlice.bytes.get_u8() == 0)),
            GraphValue::INTEGER => Ok(GraphValue::Integer(srcByteSlice.bytes.get_i64())),
            GraphValue::DECIMAL => Ok(GraphValue::Decimal(srcByteSlice.bytes.get_f64())),
            GraphValue::NULL => Ok(GraphValue::Null),
            _ => throwFormat!("unknown type tag:{}",typeTag)
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
            GraphValue::Null => destByteSlice.put_u8(GraphValue::NULL),
            _ => panic!("impossible")
        }

        Ok(())
    }
}

impl Serialize for GraphValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 因为没有别的地方可以传递参数来标识了 不得已用threadLocal
        if global::UNTAGGED_ENUM_JSON.get() {
            match self {
                GraphValue::Pending(s) => s.serialize(serializer),
                GraphValue::String(s) => s.serialize(serializer),
                GraphValue::Boolean(s) => s.serialize(serializer),
                GraphValue::Integer(s) => s.serialize(serializer),
                GraphValue::Decimal(s) => s.serialize(serializer),
                GraphValue::Null => serializer.serialize_none(),
                _ => panic!("impossible")
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
                GraphValue::Null => {
                    serialMap.serialize_key("Null")?;
                    serialMap.serialize_value(&Value::Null)?;
                }
                _ => panic!("impossible")
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
            Element::Null => Ok(GraphValue::Null),
            _ => throwFormat!("element:{element:?} can not be transform to GraphValue"),
        }
    }
}

pub type GraphValueType = Byte;

impl GraphValue {
    // 以下codec时候的type标识
    pub const PENDING: GraphValueType = 0;
    pub const STRING: GraphValueType = 1;
    pub const BOOLEAN: GraphValueType = 2;
    pub const INTEGER: GraphValueType = 3;
    pub const DECIMAL: GraphValueType = 4;
    pub const POINT_DESC: GraphValueType = 5;
    pub const NULL: GraphValueType = 6;

    pub const GRAPH_VALUE_DUMMY: GraphValue = GraphValue::Null;

    pub fn getDefaultValue(graphValueType: GraphValueType) -> Result<GraphValue> {
        match graphValueType {
            GraphValue::STRING => Ok(GraphValue::String(global::EMPTY_STR.to_owned())),
            GraphValue::BOOLEAN => Ok(GraphValue::Boolean(false)),
            GraphValue::INTEGER => Ok(GraphValue::Integer(0)),
            GraphValue::DECIMAL => Ok(GraphValue::Decimal(0.0)),
            _ => throwFormat!("unsupported graphValueType:{}", graphValueType)
        }
    }

    /// 当前不支持 type的自动转换 两边的type要严格相同的
    pub fn calc(&self, op: Op, rightValues: &[GraphValue]) -> Result<GraphValue> {
        if let Op::SqlOp(SqlOp::In) = op {
            self.calcIn(rightValues)
        } else {
            // 当前只允许in的时候有多个
            if rightValues.len() > 1 {
                throw!("right values only can be multi when op is in");
            }

            self.calcOneToOne(op, &rightValues[0])
        }
    }

    // todo calc0的时候是不是应该拦掉 like ‘%a’ 和 like ‘%a%’
    pub fn calc0(&self, op: Op, rightValues: &[GraphValue]) -> Result<GraphValue> {
        if rightValues.len() > 1 {
            if let Op::SqlOp(SqlOp::In) = op {
                for rightValue in rightValues {
                    match rightValue { // rightValues需要的都是常量
                        GraphValue::Pending(_) | GraphValue::IndexUseful { .. } | GraphValue::IndexUseless => return Ok(GraphValue::IndexUseless),
                        _ => {}
                    }
                }

                // rightValues都是常量
                match self {
                    GraphValue::Pending(columnName) => {
                        Ok(GraphValue::IndexUseful {
                            columnName: columnName.clone(),
                            op,
                            values: rightValues.to_vec(),
                        })
                    }
                    GraphValue::IndexUseful { .. } | GraphValue::IndexUseless => Ok(GraphValue::IndexUseless),
                    GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null => {
                        self.calc(op, rightValues)
                    }
                }
            } else {
                throw!("right values only can be multi when op is in")
            }
        } else {
            let rightValue = &rightValues[0];

            match (self, rightValue) {
                (GraphValue::Pending(columnName), GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null) => {
                    if op.permitByIndex() {
                        Ok(GraphValue::IndexUseful {
                            columnName: columnName.clone(),
                            op,
                            values: vec![rightValue.clone()],
                        })
                    } else {  // a+3 显然是不能的
                        Ok(GraphValue::IndexUseless)
                    }
                }
                (GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null, GraphValue::Pending(columnName)) => {
                    if op.permitByIndex() {
                        Ok(GraphValue::IndexUseful {
                            columnName: columnName.clone(),
                            op,
                            values: vec![self.clone()],
                        })
                    } else {  // a+3 显然是不能的
                        Ok(GraphValue::IndexUseless)
                    }
                }
                // 两边都是常量
                (GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null,
                    GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null) => {
                    self.calc(op, &[rightValue.clone()])
                }
                _ => Ok(GraphValue::IndexUseless)
            }
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
                            (GraphValue::Null, GraphValue::Null) => Ok(GraphValue::Boolean(true)),
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
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                    MathCalcOp::Divide => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer / integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 / (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float / float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 / (*integer as f64))),
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                    MathCalcOp::Multiply => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer * integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 * (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float * float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 * (*integer as f64))),
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                    MathCalcOp::Minus => {
                        match (self, rightValue) {
                            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Ok(GraphValue::Integer(integer - integer0)),
                            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Ok(GraphValue::Decimal(float64 - (*integer as f64))),
                            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Ok(GraphValue::Decimal(float - float0)),
                            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Ok(GraphValue::Decimal(float64 - (*integer as f64))),
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                }
            }
            Op::LogicalOp(logicalOp) => {
                match logicalOp {
                    LogicalOp::Or => {
                        match (self, rightValue) {
                            (GraphValue::Boolean(bool), GraphValue::Boolean(bool0)) => Ok(GraphValue::Boolean(bool | bool0)),
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                    LogicalOp::And => {
                        match (self, rightValue) {
                            (GraphValue::Boolean(bool), GraphValue::Boolean(bool0)) => Ok(GraphValue::Boolean(bool & bool0)),
                            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
                        }
                    }
                }
            }
            Op::SqlOp(SqlOp::Like) => { // todo 实现对like的计算 完成
                match (self, rightValue) {
                    (GraphValue::String(selfString), GraphValue::String(rightString)) => {
                        match determineLikePattern(rightString)? {
                            LikePattern::Equal(s) => Ok(GraphValue::Boolean(selfString == rightString)),
                            LikePattern::Redundant => Ok(GraphValue::Boolean(true)),
                            LikePattern::StartWith(s) => Ok(GraphValue::Boolean(selfString.starts_with(&s))),
                            LikePattern::Contain(s) => Ok(GraphValue::Boolean(selfString.contains(&s))),
                            LikePattern::EndWith(s) => Ok(GraphValue::Boolean(selfString.ends_with(&s))),
                        }
                    }
                    (GraphValue::Null, GraphValue::Null) => Ok(GraphValue::Boolean(true)),
                    (GraphValue::Null, GraphValue::String(_)) => Ok(GraphValue::Boolean(false)),
                    (GraphValue::String(_), GraphValue::Null) => Ok(GraphValue::Boolean(false)),
                    _ => throwFormat!("like can only used between strings")
                }
            }
            _ => throwFormat!("can not use {op:?}, between {self:?} , {rightValue:?}"),
        }
    }

    fn calcIn(&self, rightValues: &[GraphValue]) -> Result<GraphValue> {
        for rightValue in rightValues {
            let calcResult = self.calcOneToOne(Op::MathCmpOp(MathCmpOp::Equal), rightValue)?;
            if calcResult.asBoolean()? == false {
                return Ok(GraphValue::Boolean(false));
            }
        }

        Ok(GraphValue::Boolean(true))
    }

    pub fn isConstant(&self) -> bool {
        match self {
            GraphValue::String(_) | GraphValue::Boolean(_) | GraphValue::Integer(_) | GraphValue::Decimal(_) | GraphValue::Null => true,
            _ => false
        }
    }

    pub fn asIndexUseful(&self) -> Result<(&Op, &[GraphValue])> {
        if let GraphValue::IndexUseful { op, values, .. } = self {
            Ok((op, values))
        } else {
            throw!("not indexUseful")
        }
    }

    pub fn asBoolean(&self) -> Result<bool> {
        if let GraphValue::Boolean(b) = self {
            return Ok(*b);
        }

        throw!("not boolean")
    }

    pub fn asString(&self) -> Result<&String> {
        if let GraphValue::String(s) = self {
            return Ok(s);
        }

        throw!("not string")
    }

    pub fn getType(&self) -> GraphValueType {
        match self {
            GraphValue::Pending(_) => Self::PENDING,
            GraphValue::String(_) => Self::STRING,
            GraphValue::Boolean(_) => Self::BOOLEAN,
            GraphValue::Integer(_) => Self::INTEGER,
            GraphValue::Decimal(_) => Self::DECIMAL,
            GraphValue::Null => Self::NULL,
            _ => { panic!() }
        }
    }
}

impl PartialEq for GraphValue {
    fn eq(&self, other: &Self) -> bool {
        self.calc(Op::MathCmpOp(MathCmpOp::Equal), &[other.clone()]).unwrap().asBoolean().unwrap()
    }
}

impl PartialOrd for GraphValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (GraphValue::String(s), GraphValue::String(s0)) => Some(s.cmp(s0)),
            (GraphValue::Boolean(b), GraphValue::Boolean(b0)) => Some(b.cmp(b0)),
            (GraphValue::Integer(integer), GraphValue::Integer(integer0)) => Some(integer.cmp(integer0)),
            (GraphValue::Decimal(float64), GraphValue::Integer(integer)) => Some(float64.total_cmp(&(*integer as f64))),
            (GraphValue::Decimal(float), GraphValue::Decimal(float0)) => Some(float.total_cmp(float0)),
            (GraphValue::Integer(integer), GraphValue::Decimal(float64)) => Some(float64.total_cmp(&(*integer as f64))),
            (GraphValue::Null, GraphValue::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::global;
    use crate::graph_value::GraphValue;
    use crate::JSON_ENUM_UNTAGGED;

    #[test]
    pub fn testNull() {
        JSON_ENUM_UNTAGGED!(println!("{}", serde_json::to_string(&GraphValue::Null).unwrap()));
    }
}