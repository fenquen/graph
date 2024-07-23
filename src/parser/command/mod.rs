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
    DropTable(String),
    DropRelation(String),
    DropIndex(String),
    CreateIndex(Index),
    CreateRelation(Table),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Select(Select),
    Link(Link),
    Unlink(Unlink),
    Commit,
    Rollback,
    Set(Set),
    ShowTables,
    ShowRelations,
    /// Option<(DBObject)> 意思是要找的index是在那个table还是relaiton上边
    ShowIndice(Option<(DBObject)>),
    Alter(Alter),
}

impl Command {
    pub fn needTx(&self) -> bool {
        match self {
            Command::Select(_) => true,
            Command::DropIndex(_) => false,
            // DropTable DropRelation 需要tx的原因是 他们需要到snapshot基底的iterator来寻找pointer key
            Command::DropTable(_) => true,
            Command::DropRelation(_) => true,
            _ => self.isDml()
        }
    }

    pub fn isDml(&self) -> bool {
        match self {
            Command::Insert(_) | Command::Link(_) | Command::Update(_) | Command::Delete(_) | Command::Unlink(_) => true,
            _ => false
        }
    }

    pub fn isDdl(&self) -> bool {
        match self {
            Command::CreateTable(_) | Command::CreateIndex(_) | Command::CreateRelation(_) => true,
            _ => false
        }
    }
}
