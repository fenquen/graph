use std::fmt::format;
use crate::parser::command::Command;
use crate::parser::Parser;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::meta::Column;

#[derive(Debug, Serialize, Deserialize)]
pub enum Alter {
    AlterIndex {},
    AlterTable(AlterTable),
    AlterRelation {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AlterTable {
    DropColumns {
        tableName: String,
        columnNames: Vec<String>,
    },
    AddColumns {
        tableName: String,
        columns: Vec<Column>,
    },
    Rename(String),
}

impl Parser {
    /// alter table car add columns (id integer not null default 0,name string)
    pub(in crate::parser) fn parseAlter(&mut self) -> Result<Command> {
        let alter =
            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                "table" => {
                    let tableName = self.getCurrentElementAdvance()?.expectTextLiteralSilent()?;

                    match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                        "add" => {
                            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                                "columns" => Alter::AlterTable(AlterTable::AddColumns {
                                    tableName,
                                    columns: self.parseColumnDefinitions()?,
                                }),
                                _ => self.throwSyntaxErrorDetail("not support")?
                            }
                        }
                        "drop" => {
                            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                                "columns" => Alter::AlterTable(AlterTable::DropColumns {
                                    tableName,
                                    columnNames: self.parseInsertColumnNames()?,
                                }),
                                _ => self.throwSyntaxErrorDetail("not support")?
                            }
                        }
                        _ => self.throwSyntaxErrorDetail("not support")?
                    }
                }
                _ => self.throwSyntaxErrorDetail("not support")?
            };

        Ok(Command::Alter(alter))
    }
}