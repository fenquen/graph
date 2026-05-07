use std::sync::atomic::Ordering;
use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use bumpalo::Bump;
use crate::parser::command::manage::Set;
use crate::{config, meta, throw};

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
            Set::SetScanConcurrency(scanConcurrency) =>
                self.session.scanConcurrency = *scanConcurrency,
            Set::SetTxUndergoingMaxCount(txUndergoingMaxCount) => {
                assert!(0 < *txUndergoingMaxCount);
                config::CONFIG.flyingTxMaxCount.store(*txUndergoingMaxCount, Ordering::Release);
            }
            Set::SetSessionMemorySize(sessionMemorySize) =>
                self.session.bump = Bump::with_capacity(*sessionMemorySize),
            Set::SetTrueFalse(s, b) => {
                match s.as_str() {
                    "auto_commit" => self.session.setAutoCommit(*b)?,
                    "stream_mode" => {
                        self.session.streamMode = *b;

                        if self.session.streamMode {
                            self.session.lastDataKey = Some(meta::DATA_KEY_INVALID);
                        } else {
                            self.session.lastDataKey = None;
                        }
                    }
                    _ => throw!(&format!("{:?} not supported", set))
                }
            }
            _ => throw!(&format!("{:?} not supported", set))
        }

        Ok(CommandExecResult::None)
    }
}