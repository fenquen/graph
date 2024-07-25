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
        cascade: bool,
        columnNames: Vec<String>,
    },
    AddColumns {
        tableName: String,
        columns: Vec<Column>,
    },
    Rename(String),
}

impl Parser {
    pub(in crate::parser) fn parseAlter(&mut self) -> Result<Command> {
        let alter =
            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                "table" => {
                    let tableName = self.getCurrentElementAdvance()?.expectTextLiteralSilent()?;

                    match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                        "add" => {
                            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                                // alter table car add columns (id integer not null default 0,name string)
                                "columns" => Alter::AlterTable(AlterTable::AddColumns {
                                    tableName,
                                    columns: self.parseColumnDefinitions()?,
                                }),
                                _ => self.throwSyntaxErrorDetail("not support")?
                            }
                        }
                        "drop" => {
                            match self.getCurrentElementAdvance()?.expectTextLiteralSilent()?.to_lowercase().as_str() {
                                // alter table car drop columns (id ,name)
                                "columns" => Alter::AlterTable(AlterTable::DropColumns {
                                    tableName,
                                    cascade: self.getCurrentElement()?.expectTextLiteralContentIgnoreCaseBool("cascade"),
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