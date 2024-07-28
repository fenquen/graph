use crate::{global, throw, throwFormat, utils};
use crate::meta::{Column, DBObject, Index, Table, TableType};
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::Parser;
use anyhow::Result;
use crate::graph_value::GraphValue;

impl Parser {
    pub(in crate::parser) fn parseCreate(&mut self) -> Result<Command> {
        let dbObjectType = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();
        match dbObjectType.as_str() {
            DBObject::RELATION | DBObject::TABLE => self.parseCreateTable(dbObjectType.as_str()),
            DBObject::INDEX => self.parseCreateIndex(),
            _ => self.throwSyntaxErrorDetail(&format!("unknow database object {}", dbObjectType))?
        }
    }

    /// create table if not exist user (id integer not null default 0,name string) <br>
    /// 因为relation和table的结构是相同的 共用
    fn parseCreateTable(&mut self, dbObjectType: &str) -> Result<Command> {
        let mut table = Table::default();

        // 应对 if not exist
        if self.getCurrentElement()?.expectTextLiteralContentIgnoreCaseBool("if") {
            self.skipElement(1)?;

            let errMessage = "you should wirte \"if not exist\" after create table";
            match self.getCurrentElementAdvance()? {
                Element::Not => {}
                _ => self.throwSyntaxErrorDetail(errMessage)?
            }
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("exist", errMessage)?;

            table.createIfNotExist = true;
        }

        // 读取table name
        table.name = self.getCurrentElementAdvance()?.expectTextLiteral("table name can not be pure number")?;

        // table名不能胡乱
        self.checkDbObjectName(&table.name)?;

        table.columns = self.parseColumnDefinitions()?;

        if dbObjectType == DBObject::TABLE {
            Ok(Command::CreateTable(table))
        } else {
            Ok(Command::CreateRelation(table))
        }
    }

    pub(super) fn parseColumnDefinitions(&mut self) -> Result<Vec<Column>> {
        let mut columns = Vec::new();

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

            match element.unwrap() {
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
                            // 对应columnType的 from_str
                            column.type0 = text.as_str().parse()?;

                            // 应对跟在column type 后边的 not null 和 default value

                            // 如何应对 ReadNull ReadDefault 灵活的变换先后顺序
                            #[derive(PartialEq)]
                            enum ReadColumnConstrainState {
                                ReadNot,
                                ReadNull,
                                ReadDefault,
                            }

                            let mut not = false;
                            let mut expectElementTypeVec = vec![];

                            loop {
                                // 读取next元素以明确起始的应该是哪个
                                let currentElement = self.getCurrentElementAdvance()?;

                                if expectElementTypeVec.is_empty() == false {
                                    if expectElementTypeVec.iter().any(|elementType| elementType == &currentElement.getType()) == false {
                                        return self.throwSyntaxError()?;
                                    }
                                }

                                expectElementTypeVec = vec![];

                                let state = match currentElement {
                                    Element::Not => ReadColumnConstrainState::ReadNot,
                                    Element::Null => ReadColumnConstrainState::ReadNull,
                                    Element::Default => ReadColumnConstrainState::ReadDefault,
                                    _ => {
                                        // 应该是逗号 留给了下边的ReadComplete
                                        self.skipElement(-1)?;
                                        readColumnState = ReadColumnState::ReadComplete;
                                        break;
                                    }
                                };

                                match state {
                                    ReadColumnConstrainState::ReadNot => {
                                        not = true;
                                        expectElementTypeVec = vec![Element::NULL];
                                    }
                                    ReadColumnConstrainState::ReadNull => {
                                        if not {
                                            column.nullable = false;
                                        } else {
                                            column.nullable = true;
                                        }
                                    }
                                    ReadColumnConstrainState::ReadDefault => {
                                        // column type 要和 default value 兼容
                                        let element = self.getCurrentElementAdvance()?;
                                        column.type0.shouldCompatibleWithElement(element)?;
                                        column.defaultValue = Some(element.clone());
                                    }
                                }
                            }
                        }
                        ReadColumnState::ReadComplete => {
                            match text.as_str() {
                                global::逗号_STR => {
                                    readColumnState = ReadColumnState::ReadColumnName;

                                    columns.push(column);
                                    column = Column::default();

                                    continue;
                                }
                                global::圆括号1_STR => {
                                    columns.push(column);
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

        if utils::hasDup(&mut columns,
                         |prev, next| prev.name.cmp(&next.name),
                         |prev, next| prev.name == next.name) {
            throw!("has duplicated column names");
        }

        Ok(columns)
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