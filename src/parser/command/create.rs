use crate::global;
use crate::meta::{Column, Table, TableType};
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::Parser;

impl Parser {
    // todo 实现 default value
    // todo 实现 if not exist 完成
    // CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)
    pub(in crate::parser) fn parseCreate(&mut self) -> anyhow::Result<Command> {
        // 不是table便是relation
        let tableType = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase().as_str().parse()?;
        self.parseCreateTable(tableType)
    }

    fn parseCreateTable(&mut self, tableType: TableType) -> anyhow::Result<Command> {
        let mut table = Table::default();

        // 应对 if not exist
        if self.getCurrentElement()?.expectTextLiteralContentIgnoreCaseBool("if") {
            self.skipElement(1)?;

            let errMessage = "you should wirte \"if not exist\" after create table";
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("not", errMessage)?;
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("exist", errMessage)?;

            table.createIfNotExist = true;
        }

        table.type0 = tableType;

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

        Ok(Command::CreateTable(table))
    }

    /// 字母数字 且 数字不能打头
    fn checkDbObjectName(&self, name: &str) -> anyhow::Result<()> {
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