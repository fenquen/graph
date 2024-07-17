use crate::parser::Parser;
use anyhow::Result;
use crate::global;
use crate::meta::DBObject;
use crate::parser::command::Command;

impl Parser {
    pub(in crate::parser) fn parseDrop(&mut self) -> Result<Command> {
        let dbObjectType = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();
        let dbObjectName = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;

        match dbObjectType.as_str() {
            DBObject::INDEX => Ok(Command::DropIndex(dbObjectName)),
            DBObject::RELATION => Ok(Command::DropRelation(dbObjectName)),
            DBObject::TABLE => Ok(Command::DropTable(dbObjectName)),
            _ => self.throwSyntaxErrorDetail(&format!("unknown db object type:{}", dbObjectType))
        }
    }
}