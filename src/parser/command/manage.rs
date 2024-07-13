use std::thread;
use crate::parser::command::Command;
use crate::parser::Parser;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::config::Config;
use crate::global;
use crate::parser::element::Element;

#[derive(Debug, Serialize, Deserialize)]
pub enum Set {
    SetAutoCommit(bool),
    SetScanConcurrency(usize),
    SetTxUndergoingMaxCount(usize),
    SetSessionMemorySize(usize),
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
        let targetName = targetName.as_str();

        match targetName {
            "scanconcurrency" | "txundergoingmaxcount" | "sessionmemorysize" => {
                if let Element::IntegerLiteral(value) = self.getCurrentElementAdvance()? {
                    let value = *value;
                    if value <= 0 {
                        self.throwSyntaxErrorDetail("value should be positive")?;
                    }

                    let mut value = value as usize;

                    match targetName {
                        "scanconcurrency" => {
                            // 原来是使用num_cpus的 后来得知rust 1.81 也有 它们都是通过 cgroup sched_getaffinity taolu
                            let cpuLogicalCoreCount = thread::available_parallelism()?.get();

                            if value > cpuLogicalCoreCount {
                                value = cpuLogicalCoreCount;
                            }

                            Ok(Command::Set(Set::SetScanConcurrency(value)))
                        }
                        "txundergoingmaxcount" => {
                            if value < Config::MIN_TX_UNDERGOING_MAX_COUNT {
                                value = Config::MIN_TX_UNDERGOING_MAX_COUNT;
                            }

                            Ok(Command::Set(Set::SetTxUndergoingMaxCount(value)))
                        }
                        "sessionmemorysize" => {
                            if value < Config::MIN_SESSION_MEMORY_SIZE {
                                value = Config::MIN_SESSION_MEMORY_SIZE;
                            }

                            Ok(Command::Set(Set::SetSessionMemorySize(value)))
                        }
                        _ => self.throwSyntaxErrorDetail(&format!("set {} not supported", targetName))?,
                    }
                } else {
                    self.throwSyntaxErrorDetail("value should be integer")?
                }
            }
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
            _ => self.throwSyntaxErrorDetail(&format!("set {} not supported", targetName))?,
        }
    }
}