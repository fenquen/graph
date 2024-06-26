use crate::global;
use crate::meta::{Column, DBObject, Index, Table, TableType};
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::Parser;
use anyhow::Result;

impl Parser {
    // todo 实现 default value
    // todo 实现 if not exist 完成
    // CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)
    pub(in crate::parser) fn parseCreate(&mut self) -> Result<Command> {
        let dbObjectType = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();
        match dbObjectType.as_str() {
            DBObject::RELATION | DBObject::TABLE => self.parseCreateTable(dbObjectType.as_str()),
            DBObject::INDEX => self.parseCreateIndex(),
            _ => self.throwSyntaxErrorDetail(&format!("unknow database object {}", dbObjectType))?
        }
    }

    /// 因为relation和table的结构是相同的 共用
    fn parseCreateTable(&mut self, dbObjectType: &str) -> Result<Command> {
        let mut table = Table::default();

        // 应对 if not exist
        if self.getCurrentElement()?.expectTextLiteralContentIgnoreCaseBool("if") {
            self.skipElement(1)?;

            let errMessage = "you should wirte \"if not exist\" after create table";
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("not", errMessage)?;
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("exist", errMessage)?;

            table.createIfNotExist = true;
        }

        // 读取table name
        table.name = self.getCurrentElementAdvance()?.expectTextLiteral("table name can not be pure number")?;

        // table名不能胡乱
        self.checkDbObjectName(&table.name)?;

        // 应该是"("
        self.getCurrentElementAdvance()?.expectTextLiteralContent(global::圆括号_STR)?;

        // 循环读取 column
        enum ReadColumnState {
            ReadColumnName,
            ReadColumnType,
            ReadComplete,
        }

        let mut readColumnState = ReadColumnState::ReadColumnName;
        let mut column = Column::default();
        loop {
            let element = self.getCurrentElementAdvanceOption();
            if element.is_none() {
                break;
            }

            let element = element.unwrap();
            match element {
                Element::TextLiteral(text) => {
                    // 砍断和text->element->&mut self联系 不然下边的throwSyntaxErrorDetail报错因为是&self的
                    let text = text.to_string();

                    match readColumnState {
                        ReadColumnState::ReadColumnName => {
                            self.checkDbObjectName(&text)?;
                            column.name = text;
                            readColumnState = ReadColumnState::ReadColumnType;
                        }
                        ReadColumnState::ReadColumnType => {
                            column.type0 = text.as_str().parse()?;
                            readColumnState = ReadColumnState::ReadComplete;

                            // 应对 null
                            // 读取下个element
                            if let Element::Null = self.getCurrentElement()? {
                                self.skipElement(1)?;
                                column.nullable = true;
                            }
                        }
                        ReadColumnState::ReadComplete => {
                            match text.as_str() {
                                global::逗号_STR => {
                                    readColumnState = ReadColumnState::ReadColumnName;

                                    table.columns.push(column);
                                    column = Column::default();

                                    continue;
                                }
                                global::圆括号1_STR => {
                                    table.columns.push(column);
                                    break;
                                }
                                _ => self.throwSyntaxError()?,
                            }
                        }
                    }
                }
                _ => self.throwSyntaxErrorDetail("column name, column type can not be pure number")?,
            }
        }

        if dbObjectType == DBObject::TABLE {
            Ok(Command::CreateTable(table))
        } else {
            Ok(Command::CreateRelation(table))
        }
    }

    /// ```create index aaa on user[id, name] ```
    fn parseCreateIndex(&mut self) -> Result<Command> {
        let mut index = Index::default();

        index.name = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;

        // 读取 on
        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("on", "index name should followed by on")?;

        index.tableName = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;

        // 读取[
        self.getCurrentElementAdvance()?.expectTextLiteral(global::方括号_STR)?;

        loop {
            let columnName = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;
            match columnName.as_str() {
                global::逗号_STR => continue,
                global::方括号1_STR => break,
                _ => {}
            }

            index.columnNames.push(columnName);
        }

        if index.columnNames.is_empty() {
            self.throwSyntaxErrorDetail("index has no columns")?;
        }

        if self.hasRemainingElement() {
            self.throwSyntaxErrorDetail("has redundant content")?;
        }

        Ok(Command::CreateIndex(index))
    }

    /// 字母数字 且 数字不能打头
    fn checkDbObjectName(&self, name: &str) -> Result<()> {
        let chars: Vec<char> = name.chars().collect();

        // 打头得要字母
        match chars[0] {
            'a'..='z' => {}
            'A'..='Z' => {}
            _ => self.throwSyntaxErrorDetail("table,column name should start with letter")?,
        }

        if name.len() == 1 {
            return Ok(());
        }

        for char in chars[1..].iter() {
            match char {
                'a'..='z' => {}
                'A'..='Z' => {}
                '0'..='9' => {}
                _ => self.throwSyntaxErrorDetail("table,column name should only contain letter , number")?,
            }
        }

        Ok(())
    }
}