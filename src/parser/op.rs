use std::fmt::{Display, Formatter};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::{global, throw};
use crate::graph_error::GraphError;
use strum_macros::{Display as DisplayStrum, Display, EnumString};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Op {
    MathCmpOp(MathCmpOp),
    SqlOp(SqlOp),
    LogicalOp(LogicalOp),
    MathCalcOp(MathCalcOp),
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::MathCmpOp(s) => write!(f, "MathCmpOp({})", s),
            Op::LogicalOp(s) => write!(f, "LogicalOp({})", s),
            Op::SqlOp(s) => write!(f, "SqlOp({})", s),
            Op::MathCalcOp(mathCalcOp) => write!(f, "MathCalcOp({})", mathCalcOp),
        }
    }
}

// https://note.qidong.name/2023/03/rust-enum-str/
#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum MathCmpOp {
    Equal,
    GreaterThan,
    GreaterEqual,
    LessEqual,
    LessThan,
    NotEqual,
}

/// "a".parse::<MathCmpOp>()用的
impl FromStr for MathCmpOp {
    type Err = GraphError;

    fn from_str(str: &str) -> std::result::Result<Self, Self::Err> {
        match str {
            global::等号_STR => Ok(MathCmpOp::Equal),
            global::小于_STR => Ok(MathCmpOp::LessThan),
            global::大于_STR => Ok(MathCmpOp::GreaterThan),
            global::小于等于_STR => Ok(MathCmpOp::LessEqual),
            global::大于等于_STR => Ok(MathCmpOp::GreaterEqual),
            global::不等_STR => Ok(MathCmpOp::NotEqual),
            _ => throw!(&format!("unknown math cmp op :{}",str)),
        }
    }
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum SqlOp {
    In,
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum MathCalcOp {
    Plus,
    Divide,
    Multiply,
    Minus,
}

impl MathCalcOp {
    pub fn fromChar(char: char) -> anyhow::Result<Self> {
        match char {
            global::加号_CHAR => Ok(MathCalcOp::Plus),
            global::除号_CHAR => Ok(MathCalcOp::Divide),
            global::乘号_CHAR => Ok(MathCalcOp::Multiply),
            global::减号_CHAR => Ok(MathCalcOp::Minus),
            _ => throw!(&format!("unknown math calc operator:{char}"))
        }
    }
}