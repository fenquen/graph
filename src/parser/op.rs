use std::fmt::{Display, Formatter};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::{global, throw, throwFormat, utils};
use crate::graph_error::GraphError;
use strum_macros::{Display as DisplayStrum, Display, EnumString};
use anyhow::Result;

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

impl Op {
    pub fn permitByIndex(&self) -> bool {
        match self {
            Op::MathCmpOp(mathCmpOp) => {
                if let MathCmpOp::NotEqual = mathCmpOp {
                    false
                } else {
                    true
                }
            }
            Op::LogicalOp(_) => false,
            Op::MathCalcOp(_) => false,
            Op::SqlOp(_) => true
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
    /// not allowed by index
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
    /// like 'a' 会在calc0的时候被消化掉变为 ='a'
    Like,
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

pub enum LikePattern {
    /// 对应 like 'a',会在calc0的时候被消化掉变为MathCmpOp::Equal<br>, like null 也会这样
    Equal(String),
    Redundant,
    StartWith(String),
    EndWith(String),
    Contain(String),
}

impl LikePattern {
    pub fn getString(&self) -> Result<&String> {
        match self {
            LikePattern::Equal(s) => Ok(s),
            LikePattern::Redundant => throw!("can not get string from LikePattern::Redundant"),
            LikePattern::StartWith(s) => Ok(s),
            LikePattern::EndWith(s) => Ok(s),
            LikePattern::Contain(s) => Ok(s),
        }
    }
}

pub fn determineLikePattern(likePattern: &str) -> Result<LikePattern> {
    // like 'a', right不包含'%', 变成equal比较
    if likePattern.contains(global::百分号_STR) == false {
        return Ok(LikePattern::Equal(likePattern.to_string()));
    }

    // right全都是'%'
    if utils::isPureSomeChar(likePattern, global::百分号_CHAR) {
        // like '%' 和 like '%%'
        if 2 >= likePattern.len() {
            return Ok(LikePattern::Redundant);
        }

        // 如果上边的不满足的话,就对应了下边的 两头都是"%"
    }
    // like '%a%',提取当中的部分,使用contains
    if likePattern.starts_with(global::百分号_STR) && likePattern.ends_with(global::百分号_STR) {
        let targetStr = &likePattern[1..likePattern.len() - 1];
        return Ok(LikePattern::Contain(targetStr.to_string()));
    }

    // like '%a' 使用 ends_with
    if likePattern.starts_with(global::百分号_STR) {
        let targetStr = &likePattern[1..];
        return Ok(LikePattern::EndWith(targetStr.to_string()));
    }

    // like 'a%' 使用 starts_with
    if likePattern.ends_with(global::百分号_STR) {
        let targetStr = &likePattern[..likePattern.len() - 1];
        return Ok(LikePattern::StartWith(targetStr.to_string()));
    }

    // 到了这边便是 like 'a%a'这样的了
    throwFormat!("like {likePattern} is not supported")
}