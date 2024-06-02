use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::global;
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::op::{MathCmpOp, Op};
use crate::parser::Parser;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Update {
    pub tableName: String,
    // todo insert的values的expr要能支持含column name的
    pub columnName_expr: HashMap<String, Expr>,
    pub filterExpr: Option<Expr>,
}

impl Parser {
    /// ```update user[name='a',order=7](id=1)```
    pub(in crate::parser) fn parseUpdate(&mut self) -> anyhow::Result<Command> {
        let mut update = Update::default();

        update.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("update should followed by table name")?;

        // []中的set values
        {
            self.getCurrentElementAdvance()?.expectTextLiteralContent(global::方括号_STR)?;
            enum State {
                ReadName,
                ReadEual,
                ReadExpr,
            }

            let mut state = State::ReadName;
            let mut parserMini = Parser::default();

            let mut columnName = None;

            'outerLoop:
            loop {
                let currentElement = self.getCurrentElementAdvance()?;

                match state {
                    State::ReadName => {
                        columnName.replace(currentElement.expectTextLiteral("expect a column name")?);

                        state = State::ReadEual;
                    }
                    State::ReadEual => {
                        if let Element::Op(Op::MathCmpOp(MathCmpOp::Equal)) = currentElement {
                            state = State::ReadExpr;
                            continue;
                        } else {
                            self.throwSyntaxErrorDetail("column name should followed by equal")?;
                        }
                    }
                    State::ReadExpr => {
                        parserMini.clear();

                        let mut elementVec = Vec::new();

                        macro_rules! getPair {
                            () => {
                                let columnName = columnName.take().unwrap();

                                parserMini.elementVecVec.push(elementVec);
                                let expr = parserMini.parseExpr(false)?;

                                update.columnName_expr.insert(columnName, expr);
                            };
                        }

                        self.skipElement(-1)?;

                        'innerLoop:
                        loop {
                            let currentElement = self.getCurrentElementAdvance()?;

                            if currentElement.expectTextLiteralContentBool(global::逗号_STR) {
                                getPair!();
                                break 'innerLoop;
                            }

                            if currentElement.expectTextLiteralContentBool(global::方括号1_STR) {
                                getPair!();
                                break 'outerLoop;
                            }

                            elementVec.push(currentElement.clone());
                        }

                        state = State::ReadName;
                    }
                }
            }
        }

        // 读取表的过滤expr
        if self.getCurrentElementOption().is_some() {
            update.filterExpr = Some(self.parseExpr(false)?);
        }

        Ok(Command::Update(update))
    }
}