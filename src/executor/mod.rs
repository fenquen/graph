use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use dashmap::mapref::one::{Ref, RefMut};
use serde_json::Value;
use strum_macros::Display;
use crate::meta::{DBObject, Index, Table};
use crate::session::Session;
use crate::{meta, throw, throwFormat};
use crate::graph_value::GraphValue;
use crate::parser::command::Command;
use crate::parser::command::manage::Set;
use crate::types::{SelectResultToFront, DBObjectId};
use anyhow::Result;

mod create;
mod insert;
mod unlink;
mod link;
mod delete;
mod update;
mod select;
mod store;
mod mvcc;
mod vaccum;
mod manage;
mod index;

#[macro_export]
macro_rules! JSON_ENUM_UNTAGGED {
    ($expr: expr) => {
        {
            global::UNTAGGED_ENUM_JSON.set(true);
            let r = $expr;
            global::UNTAGGED_ENUM_JSON.set(false);
            r
        }
    };
}

#[derive(Debug, Display)]
enum CommandExecResult {
    SelectResult(Vec<Value>),
    DmlResult,
    DdlResult,
    None,
}

pub struct CommandExecutor<'session> {
    session: &'session mut Session,
}

impl<'session> CommandExecutor<'session> {
    pub fn new(session: &'session mut Session) -> Self {
        CommandExecutor {
            session
        }
    }

    pub fn execute(&mut self, commands: &mut [Command]) -> Result<SelectResultToFront> {
        // 单个的command可能得到单个Vec<Value>
        let mut valueVecVec = Vec::with_capacity(commands.len());

        for command in commands {
            let executionResult = match command {
                Command::CreateIndex(index) => {
                    let index = Index {
                        id: DBObjectId::default(),
                        name: index.name.clone(),
                        tableName: index.tableName.clone(),
                        columnNames: index.columnNames.clone(),
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: index.createIfNotExist,
                    };

                    self.createIndex(index)?
                }
                Command::CreateTable(table) => {
                    let table = Table {
                        id: DBObjectId::default(),
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        // todo rowId 要从1起 完成
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: table.createIfNotExist,
                        indexNames: table.indexNames.clone(),
                    };

                    self.createTable(table, true)?
                }
                Command::CreateRelation(table) => {
                    let table = Table {
                        id: DBObjectId::default(),
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: table.createIfNotExist,
                        indexNames: table.indexNames.clone(),
                    };

                    self.createTable(table, false)?
                }
                Command::Insert(insertValues) => self.insert(insertValues)?,
                Command::Select(select) => self.select(select)?,
                Command::Link(link) => self.link(link)?,
                Command::Delete(delete) => self.delete(delete)?,
                Command::Update(update) => self.update(update)?,
                Command::Unlink(unlink) => self.unlink(unlink)?,
                Command::Commit => {
                    let commitResult = self.commit()?;
                    self.session.generateTx()?;
                    commitResult
                }
                Command::Rollback => {
                    let rollbackResult = self.rollback()?;
                    self.rollback()?;
                    rollbackResult
                }
                Command::Set(set) => self.set(set)?,
            };

            // 如何应对多个的select
            if let CommandExecResult::SelectResult(valueVec) = executionResult {
                println!("{}\n", serde_json::to_string(&valueVec)?);
                valueVecVec.push(valueVec);
            }
        }

        Ok(valueVecVec)
    }

    fn getDBObjectByName(&self, dbObjectName: &str) -> Result<Ref<String, DBObject>> {
        let dbObject = meta::NAME_DB_OBJ.get(dbObjectName);
        if dbObject.is_none() {
            throwFormat!("db object:{} not exist", dbObjectName);
        }

        Ok(dbObject.unwrap())
    }
}


pub enum IterationCmd {
    Break,
    Continue,
    Return,
    Nothing,
}

#[cfg(test)]
mod test {
    use std::io::{SeekFrom, Write};
    use serde::{Deserialize, Serialize, Serializer};
    use serde::ser::{SerializeMap, SerializeStruct};
    use serde_json::json;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use crate::graph_value::GraphValue;
    use crate::{byte_slice_to_u64, global, u64ToByteArrRef};

    #[test]
    pub fn a() {
        let mut rowData = json!({});
        rowData["name"] = json!(GraphValue::String("s".to_string()));
        println!("{}", serde_json::to_string(&rowData).unwrap());
    }

    #[test]
    pub fn testJsonTagged() {
        #[derive(Deserialize)]
        enum A {
            S(String),
        }

        let a = A::S("1".to_string());

        impl Serialize for A {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
            {
                match self {
                    A::S(string) => {
                        if global::UNTAGGED_ENUM_JSON.get() {
                            serializer.serialize_str(string.as_str())
                        } else {
                            // let mut s = serializer.serialize_map(Some(1usize))?;
                            // s.serialize_key("S")?;
                            // s.serialize_value(string)?;

                            let mut s = serializer.serialize_struct("AAAAA", 1)?;
                            s.serialize_field("S", string)?;
                            s.end()
                        }
                    }
                }
            }
        }

        println!("{}", serde_json::to_string(&a).unwrap());

        global::UNTAGGED_ENUM_JSON.set(true);
        println!("{}", serde_json::to_string(&a).unwrap());
    }

    #[tokio::test]
    pub async fn testWriteU64() {
        // 如果设置了append 即使再怎么seek 也只会到末尾append
        let mut file = OpenOptions::new().write(true).read(true).create(true).open("data/user").await.unwrap();
        println!("{}", file.seek(SeekFrom::Start(8)).await.unwrap());
        println!("{}", file.seek(SeekFrom::Current(0)).await.unwrap());

        file.into_std().await.write(&[9]).unwrap();
        //  file.write_u8(9).await.unwrap();
        // file.write_u64(1u64).await.unwrap();
    }

    #[test]
    pub fn testU64Codec() {
        let s = u64ToByteArrRef!(2147389121u64);

        let s1 = u64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
        let aa = byte_slice_to_u64!(s);

        println!("{},{}", s1, aa);
    }
}
