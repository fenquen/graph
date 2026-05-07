use std::{cmp, thread};
use crate::parser::command::Command;
use crate::parser::Parser;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::config::Config;
use crate::global;
use crate::parser::element::Element;

#[derive(Debug, Serialize, Deserialize)]
pub enum Set {
    SetScanConcurrency(usize),
    SetTxUndergoingMaxCount(usize),
    SetSessionMemorySize(usize),
    /// 统1处理 set auto_commit/stream_mode true/false 这样的模式
    /// 而不是单独使用  SetAutoCommit(bool) SetStreamMode(bool)
    SetTrueFalse(String, bool),
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
        let targetNameString =
            self.getCurrentElementAdvance()?
                .expectTextLiteral(global::EMPTY_STR)?
                .to_lowercase();
        
        let targetName = targetNameString.as_str();

        match targetName {
            "scan_concurrency" | "tx_undergoing_max_count" | "session_memorysize" => {
                match self.getCurrentElementAdvance()? {
                    Element::IntegerLiteral(value) => {
                        let value = *value;
                        if value <= 0 {
                            self.throwSyntaxErrorDetail("value should be positive")?;
                        }

                        let mut value = value as usize;

                        match targetName {
                            "scan_concurrency" => {
                                // 原来是使用num_cpus的 后来得知rust 1.81 也有 它们都是通过 cgroup sched_getaffinity taolu
                                let cpuLogicalCoreCount = thread::available_parallelism()?.get();

                                value = cmp::min(value, cpuLogicalCoreCount);

                                Ok(Command::Set(Set::SetScanConcurrency(value)))
                            }
                            "tx_undergoing_max_count" => {
                                if value < Config::FLYING_TX_MAX_COUNT_MIN {
                                    value = Config::FLYING_TX_MAX_COUNT_MIN;
                                }

                                Ok(Command::Set(Set::SetTxUndergoingMaxCount(value)))
                            }
                            "session_memorysize" => {
                                if value < Config::SESSION_MEMORY_SIZE_MIN {
                                    value = Config::SESSION_MEMORY_SIZE_MIN;
                                }

                                Ok(Command::Set(Set::SetSessionMemorySize(value)))
                            }
                            _ => self.throwSyntaxErrorDetail(&format!("set {} not supported", targetName))?,
                        }
                    }
                    _ => self.throwSyntaxErrorDetail("value should be integer")?,
                }
            }
            "auto_commit" | "stream_mode" => {
                let r =
                    match self.getCurrentElementAdvance()? {
                        Element::Boolean(b) => Ok(Command::Set(Set::SetTrueFalse(targetNameString, *b))),
                        Element::IntegerLiteral(n) => Ok(Command::Set(Set::SetTrueFalse(targetNameString, *n != 0))),
                        Element::TextLiteral(s) => {
                            let b =
                                match s.to_lowercase().as_str() {
                                    "on" => true,
                                    "off" => false,
                                    _ => self.throwSyntaxErrorDetail("set auto_commit should use on/off")?,
                                };

                            Ok(Command::Set(Set::SetTrueFalse(targetNameString, b)))
                        }
                        _ => self.throwSyntaxErrorDetail("set auto_commit should use true/false ,0/not 0, on/off")?,
                    };

                // 例如 set auto_commit true aa,后边还有多余的
                if self.getCurrentElementOption().is_some() {
                    self.throwSyntaxErrorDetail("has redundant tail")?
                }

                r
            }
            _ => self.throwSyntaxErrorDetail(&format!("set {} not supported", targetName))?,
        }
    }
}