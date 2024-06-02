use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::global;
use crate::parser::command::Command;
use crate::parser::Parser;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Insert {
    pub tableName: String,
    /// insert into table (column) values ('a')
    pub useExplicitColumnNames: bool,
    pub columnNames: Vec<String>,
    pub columnExprs: Vec<Expr>,
}

impl Parser {
    // insert   INTO TEST VALUES ( '0'  , ')')
    // insert into test (column1) values ('a')
    // todo 实现 insert into values(),()
    pub(in crate::parser) fn parseInsert(&mut self) -> anyhow::Result<Command> {
        let currentElement = self.getCurrentElementAdvance()?;
        if currentElement.expectTextLiteralContentIgnoreCaseBool("into") == false {
            self.throwSyntaxErrorDetail("insert should followed by into")?;
        }

        let mut insertValues = Insert::default();

        insertValues.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("table name should not pure number")?.to_string();

        loop { // loop 对应下边说的猥琐套路
            let currentText = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase();
            match currentText.as_str() {
                global::圆括号_STR => { // 各column名
                    insertValues.useExplicitColumnNames = true;

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnName都要是TextLiteral 而不是StringContent
                        let text = currentElement.expectTextLiteral(global::EMPTY_STR)?;
                        match text.as_str() {
                            global::逗号_STR => continue,
                            // columnName读取结束了 下边应该是values
                            global::圆括号1_STR => break,
                            _ => insertValues.columnNames.push(text),
                        }
                    }

                    // 后边应该到下边的 case "VALUES" 那边 因为rust的match默认有break效果不会到下边的case 需要使用猥琐的套路 把它们都包裹到loop
                }
                "VALUES" => { // values
                    insertValues.columnExprs = self.parseInExprs()?;
                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        // 如果是显式说明的columnName 需要确保columnName数量和value数量相同
        if insertValues.useExplicitColumnNames {
            if insertValues.columnNames.len() != insertValues.columnExprs.len() {
                self.throwSyntaxErrorDetail("column number should equal value number")?;
            }

            if insertValues.columnNames.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column")?;
            }
        } else {
            if insertValues.columnExprs.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column value")?;
            }
        }

        Ok(Command::Insert(insertValues))
    }
}