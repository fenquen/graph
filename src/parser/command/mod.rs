use serde::{Deserialize, Serialize};
use crate::meta::{DBObject, Index, Table};
use crate::parser::command::alter::Alter;
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
mod drop;
mod show;
pub mod alter;

#[derive(Debug, Serialize, Deserialize)]
pub enum Command { //  todo 实现 order by
    CreateTable(Table),
    CreateIndex(Index),
    CreateRelation(Table),

    DropTable(String),
    DropRelation(String),
    DropIndex(String),

    Alter(Alter),

    Insert(Insert),
    Update(Update),
    Delete(Delete),

    Link(Link),
    Unlink(Unlink),

    Select(Select),

    Commit,
    Rollback,

    Set(Set),

    ShowTables,
    ShowRelations,
    /// Option<(DBObject)> 意思是要找的index是在那个table还是relaiton上边
    ShowIndice(Option<(DBObject)>),
}

impl Command {
    pub fn needTx(&self) -> bool {
        match self {
            Command::Select(_) => true,
            _ => self.isDml()
        }
    }

    pub fn isDml(&self) -> bool {
        match self {
            Command::Insert(_) | Command::Update(_) | Command::Delete(_) => true,
            Command::Link(_) | Command::Unlink(_) => true,
            _ => false
        }
    }

    pub fn isDdl(&self) -> bool {
        match self {
            Command::CreateTable(_) | Command::CreateIndex(_) | Command::CreateRelation(_) => true,
            Command::DropTable(_) | Command::DropIndex(_) | Command::DropRelation(_) => true,
            Command::Alter(_) => true,
            _ => false
        }
    }
}
