use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::parser::command::Command;
use crate::parser::Parser;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Delete {
    pub tableName: String,
    pub filterExpr: Option<Expr>,
}

impl Parser {
    /// delete from user(a=1)
    pub(in crate::parser) fn parseDelete(&mut self) -> anyhow::Result<Command> {
        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("from", "delete should followed by from")?;

        let mut delete = Delete::default();

        delete.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("expect a table after from")?;

        if self.getCurrentElementOption().is_some() {
            delete.filterExpr = Some(self.parseExpr(false)?);
        }

        Ok(Command::Delete(delete))
    }
}