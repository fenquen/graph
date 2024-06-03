use serde::{Deserialize, Serialize};
use crate::meta::Table;
use crate::parser::command::delete::Delete;
use crate::parser::command::insert::Insert;
use crate::parser::command::link::Link;
use crate::parser::command::manage::Set;
use crate::parser::command::select::Select;
use crate::parser::command::unlink::Unlink;
use crate::parser::command::update::Update;

pub mod create;
pub mod link;
pub mod unlink;
pub mod insert;
pub mod delete;
pub mod update;
pub mod select;
pub mod manage;

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    CreateTable(Table),
    Insert(Insert),
    Link(Link),
    Update(Update),
    Select(Select),
    Delete(Delete),
    Unlink(Unlink),
    Commit,
    Rollback,
    Set(Set),
}

impl Command {
    pub fn isDml(&self) -> bool {
        match self {
            Command::Insert(_) | Command::Link(_) | Command::Update(_) | Command::Unlink(_) => true,
            _ => false
        }
    }
}
