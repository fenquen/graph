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
    pub fn collectColNameValue(&self, dest: &mut HashMap<String, Vec<(Op, Vec<GraphValue>)>>, isAnd: &mut bool) -> Result<GraphValue> {
        match self {
            Expr::Single(element) => {
                Ok(GraphValue::try_from(element)?)
            }
            Expr::BiDirection { leftExpr, op, rightExprs } => {
                let leftValue = leftExpr.collectColNameValue(dest, isAnd)?;

                let rightValues: Vec<GraphValue> =
                    rightExprs.iter().map(|rightExpr| { rightExpr.collectColNameValue(dest, isAnd).unwrap() }).collect();

                let graphValueIndex = leftValue.calc0(op.clone(), &rightValues)?;

                if let GraphValue::IndexUseful { ref columnName, op, ref values } = graphValueIndex {
                    let mut op_valuesVec = dest.getMutWithDefault(columnName);

                    // 拆分掉in
                    if let Op::SqlOp(SqlOp::In) = op {
                        if values.len() == 1 {
                            op_valuesVec.push((Op::MathCmpOp(MathCmpOp::Equal), values.clone()));
                        } else {  // 要是in有多个的话 需要是or
                            *isAnd = false;

                            for value in values {
                                op_valuesVec.push((Op::MathCmpOp(MathCmpOp::Equal), vec![value.clone()]));
                            }
                        }
                    } else {
                        op_valuesVec.push((op, values.clone()));
                    }
                }

                // 含有or的话 都以它来 不然都是and
                if let Op::LogicalOp(LogicalOp::Or) = op {
                    *isAnd = false;
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


