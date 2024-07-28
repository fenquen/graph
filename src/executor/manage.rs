use std::sync::atomic::Ordering;
use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use bumpalo::Bump;
use crate::parser::command::manage::Set;
use crate::{config, throw};

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
            Set::SetScanConcurrency(scanConcurrency) => self.session.scanConcurrency = *scanConcurrency,
            Set::SetTxUndergoingMaxCount(txUndergoingMaxCount) => {
                assert!(0 < *txUndergoingMaxCount);
                config::CONFIG.txUndergoingMaxCount.store(*txUndergoingMaxCount, Ordering::Release);
            }
            Set::SetSessionMemorySize(sessionMemorySize) => {
                self.session.bump = Bump::with_capacity(*sessionMemorySize);
            }
            // _ => throw!(&format!("{:?} not supported", set))
        }

        Ok(CommandExecResult::None)
    }
}