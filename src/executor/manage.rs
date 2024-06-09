use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use crate::parser::command::manage::Set;
use crate::throw;

impl<'session> CommandExecutor<'session> {
    pub(in crate::executor) fn commit(&mut self) -> Result<CommandExecResult> {
        self.session.commit()?;
        Ok(CommandExecResult::None)
    }

    pub(in crate::executor) fn rollback(&mut self) -> Result<CommandExecResult> {
        self.session.rollback()?;
        Ok(CommandExecResult::None)
    }

    pub(in crate::executor) fn set(&mut self, set: &Set) -> Result<CommandExecResult> {
        match set {
            Set::SetAutoCommit(b) => self.session.setAutoCommit(*b)?,
            Set::SetScanConcurrency(scanConcurrency) => self.session.setScanConcurrency(*scanConcurrency)?,
            _ => throw!(&format!("{:?} not supported", set))
        }

        //self.session.setAutoCommit()
        Ok(CommandExecResult::None)
    }
}