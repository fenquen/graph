use crate::parser::Parser;
use anyhow::Result;
use crate::global;
use crate::meta::{DBObject, Table};
use crate::parser::command::Command;

impl Parser {
    pub(in crate::parser) fn parseShow(&mut self) -> Result<Command> {
        match self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase().as_str() {
            "indice" => {
                if self.getCurrentElementOption().is_some() {
                    self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("on", global::EMPTY_STR)?;

                    let dbObjectTypeString = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();

                    match dbObjectTypeString.as_str() {
                        DBObject::TABLE | DBObject::RELATION => {
                            let mut table = Table::default();
                            table.name = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;

                            match dbObjectTypeString.as_str() {
                                DBObject::TABLE => Ok(Command::ShowIndice(Some(DBObject::Table(table)))),
                                DBObject::RELATION => Ok(Command::ShowIndice(Some(DBObject::Relation(table)))),
                                _ => panic!("impossible")
                            }
                        }
                        _ => self.throwSyntaxErrorDetail("you should show indice on table/relation")?
                    }
                } else {
                    Ok(Command::ShowIndice(None))
                }
            }
            "tables" => Ok(Command::ShowTables),
            "relations" => Ok(Command::ShowRelations),
            _ => self.throwSyntaxError()?
        }
    }
}