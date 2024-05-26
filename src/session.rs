use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use crate::{command_executor, global, meta, parser, throw};
use anyhow::Result;
use log::log;
use rocksdb::{BoundColumnFamily, DB, OptimisticTransactionDB, Options, Transaction};
use tokio::io::AsyncWriteExt;
use crate::command_executor::{CommandExecutor, SelectResultToFront};
use crate::global::Byte;
use crate::meta::TxId;
use crate::parser::{Command, SqlOp};

pub struct Session {
    autoCommit: bool,
    // currentTx: Option<Transaction<'db, OptimisticTransactionDB>>,
    txId: Option<TxId>,
    pub mutations: RefCell<HashMap<String, Vec<Mutation>>>,
    pub dataStore: &'static DB,
}

impl Session {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn begin(&mut self) -> Result<()> {
        if self.txId.is_some() {
            throw!("you have not commit a previous tx");
        }

        self.txId = Some(meta::TX_ID_COUNTER.fetch_add(1, Ordering::AcqRel));

        Ok(())
    }

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub fn executeSql(&mut self, sql: &str) -> Result<SelectResultToFront> {
        let mut commands = parser::parse(sql)?;

        if commands.is_empty() {
            return Ok(vec![]);
        }

        if self.autoCommit {
            self.begin()?;
        } else {
            if self.txId.is_none() {
                throw!("manaul commit mode, but not in a transaction")
            }
        }

        let selectResultToFront = self.executeCommands(&mut commands)?;

        if self.autoCommit {
            self.commit()?;
        }

        Ok(selectResultToFront)
    }

    fn executeCommands(&mut self, commands: &mut [Command]) -> Result<SelectResultToFront> {
        let commandExecutor = CommandExecutor::new(self);
        commandExecutor.execute(commands)
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.txId.is_none() {
            throw!("not in a transaction")
        }

        self.txId = None;
        self.mutations.borrow_mut().clear();

        Ok(())
    }

    pub fn setAutoCommit(&mut self, autoCommit: bool) {
        self.autoCommit = autoCommit;
    }

    pub fn getTxId(&self) -> Result<TxId> {
        match self.txId {
            Some(txId) => Ok(txId),
            None => throw!("not in a transaction")
        }
    }

    pub fn getColFamily(&self, colFamilyName: &str) -> Result<Arc<BoundColumnFamily>> {
        match meta::STORE.dataStore.cf_handle(colFamilyName) {
            Some(cf) => Ok(cf),
            None => throw!(&format!("column family:{} not exist", colFamilyName))
        }
    }

    pub fn createColFamily(&self, columnFamilyName: &str) -> Result<()> {
        Ok(meta::STORE.dataStore.create_cf(columnFamilyName, &Options::default())?)
    }

    pub fn wtiteAddMutation(&self,
                            tableName: &String,
                            data: KV, xmin: KV, xmax: KV) {
        let addMutation = Mutation::ADD {
            data,
            xmin,
            xmax,
        };

        self.writeMutation(tableName, addMutation);
    }

    pub fn writeUpdateMutation(&self,
                               tableName: &String,
                               oldXmax: KV,
                               newData: KV, newXmin: KV, newXmax: KV) {
        let updateMutation = Mutation::UPDATE {
            oldXmax,
            newData,
            newXmin,
            newXmax,
        };

        self.writeMutation(tableName, updateMutation);
    }

    pub fn writeDeleteMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, Mutation::DELETE { oldXmax })
    }

    fn writeMutation(&self, tableName: &String, mutation: Mutation) {
        let mut mutations = self.mutations.borrow_mut();
        let tableMutations = mutations.getMutWithDefault(tableName);
        tableMutations.push(mutation);
        //match mutation {
        //  Mutation::ADD { .. } => tableMutation.addMutations.push(mutation),
        //Mutation::UPDATE { .. } => tableMutation.updateMutations.push(mutation),
        //Mutation::DELETE { .. } => tableMutation.deleteMutations.push(mutation),
        //}
    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            autoCommit: true,
            dataStore: &meta::STORE.dataStore,
            mutations: Default::default(),
            txId: None,
        }
    }
}

trait HashMapExt<K, V, S = RandomState> {
    fn getMutWithDefault<Q: ?Sized>(&mut self, k: &Q) -> &mut V
        where K: Borrow<Q> + From<Q>,
              Q: Hash + Eq + Clone,
              V: Default;
}

impl<K: Eq + Hash, V, S: BuildHasher> HashMapExt<K, V, S> for HashMap<K, V, S> {
    fn getMutWithDefault<Q: ?Sized>(&mut self, k: &Q) -> &mut V
        where K: Borrow<Q> + From<Q>,
              Q: Hash + Eq + Clone,
              V: Default {
        if let None = self.get_mut(k) {
            self.insert(k.clone().into(), V::default());
        }
        self.get_mut(k).unwrap()
    }
}

pub type KV = (Vec<Byte>, Vec<Byte>);

pub enum Mutation {
    ADD {
        data: KV,
        xmin: KV,
        xmax: KV,
    },
    UPDATE {
        oldXmax: KV,
        newData: KV,
        newXmin: KV,
        newXmax: KV,
    },
    DELETE {
        oldXmax: KV
    },
}

#[derive(Default)]
pub struct TableMutation {
    addMutations: Vec<Mutation>,
    updateMutations: Vec<Mutation>,
    deleteMutations: Vec<Mutation>,
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    #[test]
    pub fn testSerialBox() {
        #[derive(Serialize, Deserialize)]
        struct Object {
            name: String,
            next: Option<Box<Object>>,
        }

        let b = Object {
            name: "b".to_string(),
            next: None,
        };

        let a = Object {
            name: "a".to_string(),
            next: Some(Box::new(b)),
        };

        println!("{}", serde_json::to_string(&a).unwrap());

        // https://stackoverflow.com/questions/26611664/what-is-the-r-operator-in-rust
        let a: Object = serde_json::from_str("").unwrap();
    }
}