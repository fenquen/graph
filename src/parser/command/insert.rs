use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::global;
use crate::parser::command::Command;
use crate::parser::Parser;
use anyhow::Result;
use crate::parser::element::Element;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Insert {
    pub tableName: String,
    /// insert into table (column) values ('a')
    pub useExplicitColumnNames: bool,
    pub columnNames: Vec<String>,
    pub columnExprVecVec: Vec<Vec<Expr>>,
}

impl Parser {
    // insert   INTO TEST VALUES ( '0'  , ')')
    // insert into test (column1) values ('a')
    // todo 实现 insert into values(),() 完成
    pub(in crate::parser) fn parseInsert(&mut self) -> Result<Command> {
        let currentElement = self.getCurrentElementAdvance()?;
        if currentElement.expectTextLiteralContentIgnoreCaseBool("into") == false {
            self.throwSyntaxErrorDetail("insert should followed by into")?;
        }

        let mut insertValues = Insert::default();

        insertValues.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("table name should not pure number")?.to_string();

        loop { // loop 对应下边说的猥琐套路
            let currentText = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();
            match currentText.as_str() {
                global::圆括号_STR => { // 各column名
                    insertValues.useExplicitColumnNames = true;

                    self.skipElement(-1);
                    insertValues.columnNames = self.parseInsertColumnNames()?;

                    if insertValues.columnNames.is_empty() {
                        self.throwSyntaxErrorDetail("you have not designate any column")?;
                    }

                    // 后边应该到下边的 case "VALUES" 那边 因为rust的match默认有break效果不会到下边的case 需要使用猥琐的套路 把它们都包裹到loop
                }
                "values" => { // values
                    loop {
                        let exprVec = self.parseInExprs()?;

                        // values里边要有东西的
                        if exprVec.is_empty() {
                            self.throwSyntaxErrorDetail("datas on single row should not be empty")?;
                        }

                        // 如果指明了column的话 那么column数量要等于value数量
                        if insertValues.useExplicitColumnNames {
                            if insertValues.columnNames.len() != exprVec.len() {
                                self.throwSyntaxErrorDetail("column count should equal with  value count")?;
                            }
                        }

                        // 确保所有的exprVec的len都相同
                        if insertValues.columnExprVecVec.len() > 0 {
                            let last = insertValues.columnExprVecVec.last().unwrap();

                            if last.len() != exprVec.len() {
                                self.throwSyntaxErrorDetail("values count should be identical")?;
                            }
                        }

                        insertValues.columnExprVecVec.push(exprVec);

                        if let Some(Element::TextLiteral(s)) = self.getCurrentElementOption() {
                            if s != global::逗号_STR {
                                break;
                            }

                            self.skipElement(1)?;
                        } else {
                            break;
                        }
                    }

                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        if insertValues.columnExprVecVec.is_empty() {
            self.throwSyntaxErrorDetail("you have not designate any column value")?;
        }

        Ok(Command::Insert(insertValues))
    }

    /// 读取 insert into test (column1) values ('a') 中 (column1) 部分
    pub(super) fn parseInsertColumnNames(&mut self) -> Result<Vec<String>> {
        self.getCurrentElementAdvance()?.expectTextLiteralContent(global::圆括号_STR)?;

        let mut columnNames = Vec::new();

        loop {
            // columnName都要是TextLiteral 而不是StringContent
            let text = self.getCurrentElementAdvance()?.expectTextLiteralSilent()?;
            match text.as_str() {
                global::逗号_STR => continue,
                // columnName读取结束了 下边应该是values
                global::圆括号1_STR => break,
                _ => columnNames.push(text),
            }
        }

        Ok(columnNames)
    }
}