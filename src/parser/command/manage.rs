use crate::parser::command::Command;
use crate::parser::Parser;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::global;
use crate::parser::element::Element;

#[derive(Debug, Serialize, Deserialize)]
pub enum Set {
    SetAutoCommit(bool),
    SetScanConcurrency(usize),
}

// todo manage体系的命令要通过sql实现 完成
impl Parser {
    pub(in crate::parser) fn parseCommit(&self) -> Result<Command> {
        // 只能有commit 后边不能有什么了
        if self.getCurrentElementOption().is_some() {
            self.throwSyntaxError()?;
        }

        Ok(Command::Commit)
    }

    pub(in crate::parser) fn parseRollback(&self) -> Result<Command> {
        // 只能有rollback 后边不能有什么了
        if self.getCurrentElementOption().is_some() {
            self.throwSyntaxError()?;
        }

        Ok(Command::Rollback)
    }

    pub(in crate::parser) fn parseSet(&mut self) -> Result<Command> {
        let targetName = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase();

        match targetName.as_str() {
            "autocommit" => {
                match self.getCurrentElementAdvance()? {
                    Element::Boolean(b) => Ok(Command::Set(Set::SetAutoCommit(*b))),
                    Element::IntegerLiteral(n) => Ok(Command::Set(Set::SetAutoCommit(*n != 0))),
                    Element::TextLiteral(s) => {
                        match s.to_lowercase().as_str() {
                            "on" => Ok(Command::Set(Set::SetAutoCommit(true))),
                            "off" => Ok(Command::Set(Set::SetAutoCommit(false))),
                            _ => self.throwSyntaxErrorDetail("set autocommit should use on/off")?,
                        }
                    }
                    _ => self.throwSyntaxErrorDetail("set autocommit should use true/false ,0/not 0, on/off")?,
                }
            }
            "scan concurrency" => {
                if let Element::IntegerLiteral(scanConcurrency) = self.getCurrentElementAdvance()? {
                    let scanConcurrency = *scanConcurrency;

                    if 0 >= scanConcurrency {
                        self.throwSyntaxErrorDetail("scan concurrency should be positive")?;
                    }

                    Ok(Command::Set(Set::SetScanConcurrency(scanConcurrency as usize)))
                } else {
                    self.throwSyntaxErrorDetail("scan concurrency should be integer")?
                }
            }
            _ => self.throwSyntaxErrorDetail(&format!("set {} not supported", targetName))?,
        }
    }
}