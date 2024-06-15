use std::collections::{HashMap, HashSet};
use std::ops::Index;
use serde_json::Value;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::graph_value::GraphValue;
use crate::parser::element::Element;
use crate::parser::op::Op;
use crate::throw;
use crate::types::RowData;

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

    pub fn a(&self, indexColumnNames: &[String],
             dest: &mut HashMap<String, Vec<(Op, GraphValue)>>) -> Result<GraphValue> {
        match self {
            Expr::Single(element) => {
                Ok(GraphValue::try_from(element)?)
            }
            Expr::BiDirection { leftExpr, op, rightExprs } => {
                let leftValue = leftExpr.a(indexColumnNames, dest)?;

                let mut columnName = None;

                let leftIsPending =
                    if let GraphValue::Pending(ref columnName0) = leftValue {
                        columnName = Some(columnName0.clone());
                        true
                    } else {
                        false
                    };

                if rightExprs.is_empty() {
                    throw!("has no right values");
                }

                let rightValues: Vec<GraphValue> = rightExprs.iter().map(|rightExpr| { rightExpr.a(indexColumnNames, dest).unwrap() }).collect();

                let mut rightHasPending = false;

                for rightValue in &rightValues {
                    // 两边都是columnName 用不了index
                    if let GraphValue::Pending(ref columnName0) = rightValue {
                        // 对in来说不能有多个的columnName
                        if rightHasPending {
                            panic!()
                        }

                        if leftIsPending {
                            panic!()
                        }

                        rightHasPending = true;

                        columnName = Some(columnName0.clone());
                    }
                }

                // 说明未用到columnName
                if columnName.is_none() {
                    panic!()
                }


                leftValue.calc(op.clone(), &rightValues)
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
                    dest.push(columnName.clone());
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


