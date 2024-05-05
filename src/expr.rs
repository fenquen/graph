use std::collections::HashMap;
use serde_json::Value;
use anyhow::Result;
use crate::graph_value::GraphValue;
use crate::parser::{Element, MathCmpOp, Op, SqlOp};
use crate::throw;

// 碰到"(" 下钻递归,返回后落地到上级的left right
#[derive(Debug,Clone)]
pub enum Expr {
    Single(Element),
    BiDirection {
        leftExpr: Box<Expr>,
        op: Op,
        rightExprVec: Vec<Box<Expr>>,
    },
    None,
}

impl Default for Expr {
    fn default() -> Self {
        Expr::None
    }
}

impl Expr {
    pub fn calc(&self, rowData: Option<&HashMap<String, GraphValue>>) -> Result<GraphValue> {
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
            Expr::BiDirection { leftExpr, op, rightExprVec } => {
                let leftValue = leftExpr.calc(rowData)?;

                if rightExprVec.is_empty() {
                    throw!("has no right values");
                }

                let rightValues: Vec<GraphValue> = rightExprVec.iter().map(|expr| { expr.calc(rowData).unwrap() }).collect();

                leftValue.calc(op.clone(), &rightValues)
            }
            Expr::None => panic!("impossible"),
        }
    }
}


