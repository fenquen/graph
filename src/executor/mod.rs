use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use dashmap::mapref::one::Ref;
use serde_json::Value;
use strum_macros::Display;
use crate::meta::{DBObject, Index, Table};
use crate::session::Session;
use crate::{meta, throwFormat};
use crate::parser::command::Command;
use crate::types::{DBObjectId, SelectResultToFront, SessionHashMap, SessionHashSet, SessionVec};
use anyhow::Result;
use bumpalo::Bump;
use bytes::BytesMut;
use graph_independent::AllocatorExt;
use crate::utils::Lengthable;

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
mod optimizer;
mod drop;
mod show;
mod alter;

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
                Command::CreateTable(table) => {
                    let table = Table {
                        id: DBObjectId::default(),
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        // todo rowId 要从1起 完成
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: table.createIfNotExist,
                        indexNames: table.indexNames.clone(),
                        invalid: table.invalid,
                    };

                    self.createTable(table, true)?
                }
                Command::DropTable(tableName) => self.dropTable(tableName)?,
                Command::DropRelation(relationName) => self.dropRelation(relationName)?,
                Command::DropIndex(indexName) => self.dropIndex(indexName, false, None)?,
                Command::CreateIndex(index) => {
                    let index = Index {
                        id: DBObjectId::default(),
                        name: index.name.clone(),
                        tableName: index.tableName.clone(),
                        columnNames: index.columnNames.clone(),
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: index.createIfNotExist,
                        invalid: index.invalid,
                    };

                    self.createIndex(index)?
                }
                Command::CreateRelation(table) => {
                    let table = Table {
                        id: DBObjectId::default(),
                        name: table.name.clone(),
                        columns: table.columns.clone(),
                        rowIdCounter: AtomicU64::new(meta::ROW_ID_MIN),
                        createIfNotExist: table.createIfNotExist,
                        indexNames: table.indexNames.clone(),
                        invalid: table.invalid,
                    };

                    self.createTable(table, false)?
                }
                Command::Insert(insert) => self.insert(insert)?,
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
                Command::Rollback => self.rollback()?,
                Command::Set(set) => self.set(set)?,
                Command::ShowIndice(dbObject) => self.showIndice(dbObject.as_ref())?,
                Command::ShowRelations => self.showRelations()?,
                Command::ShowTables => self.showTables()?,
                Command::Alter(alter) => self.alter(alter)?,
                _ => throwFormat!("unsupported command: {:?}", command)
            };

            // 如何应对多个的select
            if let CommandExecResult::SelectResult(valueVec) = executionResult {
                log::debug!("{}\n", serde_json::to_string(&valueVec)?);
                valueVecVec.push(valueVec);
            }
        }

        Ok(valueVecVec)
    }

    #[inline]
    fn newIn(&self) -> BytesMut {
        self.withCapacityIn(0)
    }

    #[inline]
    fn withCapacityIn(&self, capacity: usize) -> BytesMut {
        self.session.withCapacityIn(capacity)
    }

    // ---------------------------------------------------------

    #[inline]
    pub fn vecNewIn<T>(&self) -> SessionVec<T> {
        self.vecWithCapacityIn(0)
    }

    #[inline]
    fn vecWithCapacityIn<T>(&self, capacity: usize) -> SessionVec<T> {
        self.session.vecWithCapacityIn(capacity)
    }

    /// 注意capacity要尽量在前边, 如果这样(dataKeys, dataKeys.len)会报错提示moved
    fn collectIntoVecWithCapacity<T>(&self, intoIterator: impl IntoIterator<Item=T> + Lengthable) -> SessionVec<T> {
        let mut sessionVec = self.vecWithCapacityIn(intoIterator.length());
        IntoIterator::into_iter(intoIterator).collect_into(&mut sessionVec);
        sessionVec
    }

    fn collectVecWithCapacity<T>(&self, iterator: impl Iterator<Item=T> + Lengthable) -> SessionVec<T> {
        let mut sessionVec = self.vecWithCapacityIn(iterator.length());
        Iterator::collect_into(iterator, &mut sessionVec);
        sessionVec
    }

    fn collectVec<T>(&self, iterator: impl Iterator<Item=T>) -> SessionVec<T> {
        let mut sessionVec = self.vecNewIn();
        Iterator::collect_into(iterator, &mut sessionVec);
        sessionVec
    }

    // -----------------------------------------------------------

    #[inline]
    fn hashMapNewIn<K, V>(&self) -> SessionHashMap<K, V> {
        self.hashMapWithCapacityIn(0)
    }

    #[inline]
    fn hashMapWithCapacityIn<K, V>(&self, capacity: usize) -> SessionHashMap<K, V> {
        self.session.hashMapWithCapacityIn(capacity)
    }

    // ------------------------------------------------------------------
    #[inline]
    fn hashSetWithCapacityIn<T: Hash + Eq>(&self, capacity: usize) -> SessionHashSet<T> {
        self.session.hashSetWithCapacityIn(capacity)
    }

    #[inline]
    fn hashSetNewIn<T: Hash + Eq>(&self) -> SessionHashSet<T> {
        self.hashSetWithCapacityIn(0)
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
