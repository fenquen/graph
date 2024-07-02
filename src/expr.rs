use std::collections::{HashMap, HashSet};
use std::ops::Index;
use serde_json::Value;
use anyhow::Result;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use crate::graph_value::GraphValue;
use crate::parser::element::Element;
use crate::parser::op::{LogicalOp, MathCmpOp, Op, SqlOp};
use crate::throw;
use crate::types::RowData;
use crate::utils::HashMapExt;

// 碰到"(" 下钻递归,返回后落地到上级的left right
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Single(Element),
    BiDirection {
        leftExpr: Box<Expr>,
        op: Op,
        rightExprs: Vec<Box<Expr>>,
    },
    None,
}

impl Default for Expr {
    fn default() -> Self {
        Expr::None
    }
}

impl Expr {
    pub fn calc(&self, rowData: Option<&RowData>) -> Result<GraphValue> {
        // 需要不断的向下到Single
        match self {
            Expr::Single(element) => {
                let graphValue = GraphValue::try_from(element)?;
                match graphValue {
                    GraphValue::Pending(ref columnName) => {
                        if let Some(rowData) = rowData {
                            Ok(rowData.get(columnName).unwrap().clone())
                        } else {
                            throw!("need actual row data to get actual value")
                        }
                    }
                    _ => Ok(graphValue),
                }
            }
            Expr::BiDirection { leftExpr, op, rightExprs } => {
                let leftValue = leftExpr.calc(rowData)?;

                if rightExprs.is_empty() {
                    throw!("has no right values");
                }

                let rightValues: Vec<GraphValue> = rightExprs.iter().map(|rightExpr| { rightExpr.calc(rowData).unwrap() }).collect();

                leftValue.calc(op.clone(), &rightValues)
            }
            Expr::None => panic!("impossible"),
        }
    }

    /// 收集tableFilter上涉及到columnName的expr 把成果收集到dest对应的map
    /// 例如 ((a=1 or a=2) and (b>3 or b=1)) 会收集成为 “a”-> []
    pub fn collectColNameValue(&self,
                               columnName_opValuesVec: &mut HashMap<String, Vec<(Op, Vec<GraphValue>)>>,
                               isPureAnd: &mut bool,
                               isPureOr: &mut bool,
                               hasExprAbandonedByIndex: &mut bool,
                               columnNameExist: &mut bool) -> Result<GraphValue> {
        match self {
            Expr::Single(element) => {
                let value = GraphValue::try_from(element)?;
                if let GraphValue::Pending(_) = &value {
                    *columnNameExist = true;
                }

                Ok(value)
            }
            Expr::BiDirection { leftExpr, op, rightExprs } => {
                let leftValue = leftExpr.collectColNameValue(columnName_opValuesVec, isPureAnd, isPureOr, hasExprAbandonedByIndex, columnNameExist)?;

                let rightValues: Vec<GraphValue> =
                    rightExprs.iter().map(|rightExpr| { rightExpr.collectColNameValue(columnName_opValuesVec, isPureAnd, isPureOr, hasExprAbandonedByIndex, columnNameExist).unwrap() }).collect();

                // 如何得到 columnName + 1 这样的index不能使用的情况
                // val + (number >1 )
                // 挑选index的条件是 GraphValue::Pending + op.permitByIndex() + graphValue.isConstant
                if let GraphValue::Pending(_) = &leftValue {
                    if op.permitByIndex() == false {
                        *hasExprAbandonedByIndex = true;
                    } else {
                        for rightValue in &rightValues {
                            if rightValue.isConstant() == false {
                                *hasExprAbandonedByIndex = true;
                                break;
                            }
                        }
                    }
                }

                let graphValueIndex = leftValue.calc0(op.clone(), &rightValues)?;

                if let GraphValue::IndexUseful { ref columnName, op, ref values } = graphValueIndex {
                    let opValuesVec = columnName_opValuesVec.getMutWithDefault(columnName);

                    if let Op::SqlOp(SqlOp::In) = op {
                        // 如果in的对象只有1个 那么是equal
                        if values.len() == 1 {
                            opValuesVec.push((Op::MathCmpOp(MathCmpOp::Equal), values.clone()));
                        } else if *isPureAnd == false {
                            for value in values {
                                opValuesVec.push((Op::MathCmpOp(MathCmpOp::Equal), vec![value.clone()]));
                            }
                        } else {
                            opValuesVec.push((op, values.clone()));
                        }
                    } else {
                        opValuesVec.push((op, values.clone()));
                    }
                }

                // 含有or的话 都以它来 不然都是and
                if let Op::LogicalOp(LogicalOp::Or) = op {
                    *isPureAnd = false;
                }

                if let Op::LogicalOp(LogicalOp::And) = op {
                    *isPureOr = false;
                }

                Ok(graphValueIndex)
            }
            Expr::None => panic!("impossible"),
        }
    }

    // todo 需要能知道expr是不是含有需要实际rowData再能的Pending 完成
    /// expr的计算得到成果是不是需要实际rowData的参加
    pub fn needAcutalRowData(&self) -> bool {
        match self {
            Expr::Single(element) => {
                if let Element::TextLiteral(_) = element {
                    true
                } else {
                    false
                }
            }
            Expr::BiDirection { leftExpr, op: _op, rightExprs: rightExprVec } => {
                if leftExpr.needAcutalRowData() {
                    return true;
                }

                for rightExpr in rightExprVec {
                    if rightExpr.needAcutalRowData() {
                        return true;
                    }
                }

                false
            }
            Expr::None => panic!("impossilble")
        }
    }

    pub fn extractColumnNames(&self, dest: &mut HashSet<String>) -> Result<()> {
        match self {
            Expr::Single(element) => {
                if let Element::TextLiteral(columnName) = element {
                    dest.insert(columnName.clone());
                }

                Ok(())
            }
            Expr::BiDirection { leftExpr, rightExprs, .. } => {
                Self::extractColumnNames(&**leftExpr, dest)?;

                for rightExpr in rightExprs {
                    Self::extractColumnNames(&**rightExpr, dest)?;
                }

                Ok(())
            }
            Expr::None => panic!("impossible")
        }
    }
}


