use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash, RandomState};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
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
    pub txMutation: RefCell<TxMutation>,
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
        self.txMutation.borrow_mut().clear();

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

    pub fn writeAddDataMutation(&self,
                                tableName: &String,
                                data: KV, xmin: KV, xmax: KV) {
        let addMutation = Mutation::AddData {
            data,
            xmin,
            xmax,
        };

        self.writeMutation(tableName, true, addMutation);
    }

    pub fn writeAddPointerMutation(&self,
                                   tableName: &String,
                                   xmin: KV, xmax: KV) {
        let addMutation = Mutation::AddPointer {
            xmin,
            xmax,
        };

        self.writeMutation(tableName, false, addMutation);
    }


    pub fn writeUpdateDataMutation(&self,
                                   tableName: &String,
                                   oldXmax: KV,
                                   newData: KV, newXmin: KV, newXmax: KV) {
        let updateMutation = Mutation::UpdateData {
            oldXmax,
            newData,
            newXmin,
            newXmax,
        };

        self.writeMutation(tableName, true, updateMutation);
    }

    pub fn writeDeleteDataMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, true, Mutation::DeleteData { oldXmax })
    }

    pub fn writeDeletePointerMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, false, Mutation::DeletePointer { oldXmax })
    }

    fn writeMutation(&self, tableName: &String, ifData: bool, mutation: Mutation) {
        let mutation = Arc::new(mutation);

        let mut txMutation = self.txMutation.borrow_mut();
        txMutation.totalMutations.push(mutation.clone());

        let tableMutation = txMutation.tableName_tableMutation.getMutWithDefault(tableName);
        tableMutation.totalMutations.push(mutation.clone());

        match (ifData, &*mutation) {
            (true, Mutation::AddData { .. }) => tableMutation.addDataMutations.push(mutation.clone()),
            (true, Mutation::UpdateData { newData, newXmin, newXmax, oldXmax }) => {
                // updateData 裂变为 delete 和 update
                let deleteDataMutation =
                    Arc::new(Mutation::DeleteData {
                        oldXmax: oldXmax.clone()
                    });
                tableMutation.modifyOldDataMutations.push(deleteDataMutation);

                let addDataMutation =
                    Arc::new(Mutation::AddData {
                        data: newData.clone(),
                        xmin: newXmin.clone(),
                        xmax: newXmax.clone(),
                    });
                tableMutation.addDataMutations.push(addDataMutation);
            }
            (true, Mutation::DeleteData { .. }) => tableMutation.modifyOldDataMutations.push(mutation.clone()),
            (false, Mutation::AddPointer { .. }) => tableMutation.addPointerMutations.push(mutation.clone()),
            (false, Mutation::DeletePointer { .. }) => tableMutation.modifyOldPointerMutations.push(mutation.clone()),
            _ => {}
        }

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
            txMutation: Default::default(),
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
    AddData {
        data: KV,
        xmin: KV,
        xmax: KV,
    },
    AddPointer {
        xmin: KV,
        xmax: KV,
    },
    UpdateData {
        oldXmax: KV,
        newData: KV,
        newXmin: KV,
        newXmax: KV,
    },
    DeleteData {
        oldXmax: KV
    },
    DeletePointer {
        oldXmax: KV
    },
}

#[derive(Default)]
pub struct TxMutation {
    /// 原样
    pub totalMutations: Vec<Arc<Mutation>>,
    pub tableName_tableMutation: HashMap<String, TableMutation>,
}

impl TxMutation {
    pub fn clear(&mut self) {
        self.totalMutations.clear();
        self.tableName_tableMutation.clear();
    }
}

#[derive(Default)]
pub struct TableMutation {
    /// 原样
    pub totalMutations: Vec<Arc<Mutation>>,
    pub addDataMutations: Vec<Arc<Mutation>>,
    pub modifyOldDataMutations: Vec<Arc<Mutation>>,
    pub addPointerMutations: Vec<Arc<Mutation>>,
    pub modifyOldPointerMutations: Vec<Arc<Mutation>>,
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