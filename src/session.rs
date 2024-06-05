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
use crate::{global, meta, parser, throw, u64ToByteArrRef};
use anyhow::Result;
use log::log;
use rocksdb::{BoundColumnFamily, DB, DBAccess, DBWithThreadMode,
              MultiThreaded, OptimisticTransactionDB, Options, SnapshotWithThreadMode, Transaction, WriteBatchWithTransaction};
use tokio::io::AsyncWriteExt;
use crate::executor::CommandExecutor;
use crate::parser::command::Command;
use crate::types::{Byte, ColumnFamily, KV, SelectResultToFront, Snapshot, TableMutations, TxId};

pub struct Session {
    autoCommit: bool,
    txId: Option<TxId>,
    dataStore: &'static DB,
    pub tableName_mutations: RefCell<HashMap<String, TableMutations>>,
    snapshot: Option<Snapshot<'static>>,
}

impl Session {
    pub fn new() -> Self {
        Default::default()
    }

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub fn executeSql(&mut self, sql: &str) -> Result<SelectResultToFront> {
        let mut commands = parser::parse(sql)?;

        if commands.is_empty() {
            return Ok(vec![]);
        }

        // todo set autocommit 是不需要tx的 如果sql只包含set如何应对
        let mut isPureSetSql = false;
        for  command in &commands {
            if let Command::Set(_) = command {
                isPureSetSql = true;
                break;
            }
        }

        // 不要求是不是auto commit的 要不在tx那么生成1个tx
        if self.notInTx() && isPureSetSql == false {
            self.generateTx()?;
        }

        let selectResultToFront = CommandExecutor::new(self).execute(&mut commands)?;

        // todo sql中执行了commit导致当前tx提交后 当前不是inTx了 调用commit报错 需要commit()不要限制inTx
        if self.autoCommit && isPureSetSql == false {
            self.commit()?;
        }

        Ok(selectResultToFront)
    }

    /// 提交之后 在到下个执行sql前 session都是 not in tx 的
    pub fn commit(&mut self) -> Result<()> {
        if self.notInTx() {
            return Ok(());
        }

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for (tableName, mutations) in self.tableName_mutations.borrow().iter() {
            let colFamily = self.getColFamily(tableName)?;
            for (key, value) in mutations {
                batch.put_cf(&colFamily, key, value);
            }
        }

        // todo lastest的txId要持久化 完成
        {
            let currentTxId = self.txId.unwrap();
            // cf需要现用现取 内部使用的是read 而create cf会用到write
            let cf = self.getColFamily(meta::COLUMN_FAMILY_NAME_TX_ID)?;

            if (currentTxId - *meta::TX_ID_START_UP.getRef()) % meta::TX_UNDERGOING_MAX_COUNT as u64 == 0 {
                // TX_CONCURRENCY_MAX
                tokio::task::spawn_blocking(move || {
                    let thresholdTx = currentTxId - meta::TX_UNDERGOING_MAX_COUNT as u64;
                    CommandExecutor::vaccumData(thresholdTx);
                });

                // todo 干掉columnFamily "tx_id" 老的txId 完成
                self.dataStore.delete_range_cf(&cf, u64ToByteArrRef!(meta::TX_ID_INVALID), u64ToByteArrRef!(currentTxId))?;
            }

            // 以当前的txId为key落地到单独的columnFamil "tx_id"
            batch.put_cf(&cf, u64ToByteArrRef!(self.txId.unwrap()), global::EMPTY_BINARY);
        }

        self.dataStore.write(batch)?;

        meta::TX_UNDERGOING_COUNT.fetch_sub(1, Ordering::AcqRel);

        self.clean();

        Ok(())
    }
    // todo rollback()不要求inTx
    pub fn rollback(&mut self) -> Result<()> {
        self.needInTx()?;
        self.clean();
        Ok(())
    }

    fn clean(&mut self) {
        self.txId = None;
        self.snapshot = None;
        self.tableName_mutations.borrow_mut().clear();
    }

    fn generateTx(&mut self) -> Result<()> {
        // 函数返回的不管是Ok还是Err里边的都是previsou value
        // 要是闭包返回的是Some那么函数返回Ok 不然是Err
        if let Err(_) = meta::TX_UNDERGOING_COUNT.fetch_update(Ordering::Release, Ordering::Acquire,
                                                               |current| {
                                                                   // 满了
                                                                   if current >= meta::TX_UNDERGOING_MAX_COUNT as u64 {
                                                                       return None;
                                                                   }
                                                                   Some(current + 1)
                                                               }) {
            throw!("too many undergoing tx");
        }
        self.txId = Some(meta::TX_ID_COUNTER.fetch_add(1, Ordering::AcqRel));
        self.snapshot = Some(self.dataStore.snapshot());
        self.tableName_mutations.borrow_mut().clear();

        Ok(())
    }

    fn needInTx(&self) -> Result<()> {
        if self.notInTx() {
            throw!("not in a transaction")
        } else {
            Ok(())
        }
    }

    fn needNotInTx(&self) -> Result<()> {
        if self.notInTx() == false {
            throw!("you have not commit a previous tx")
        } else {
            Ok(())
        }
    }

    fn notInTx(&self) -> bool {
        self.txId.is_none() || self.snapshot.is_none()
    }

    /// 如果当前是 false  && in tx 那么会提交当前tx ,jdbc也是这样的
    pub fn setAutoCommit(&mut self, autoCommit: bool) -> Result<()> {
        if self.autoCommit == false && self.notInTx() == false {
            self.commit()?;
        }

        self.autoCommit = autoCommit;

        Ok(())
    }

    pub fn getTxId(&self) -> Result<TxId> {
        match self.txId {
            Some(txId) => Ok(txId),
            None => throw!("not in a transaction")
        }
    }

    pub fn getColFamily(&self, colFamilyName: &str) -> Result<ColumnFamily> {
        match meta::STORE.dataStore.cf_handle(colFamilyName) {
            Some(cf) => Ok(cf),
            None => throw!(&format!("column family:{} not exist", colFamilyName))
        }
    }

    pub fn getSnapshot(&self) -> Result<&Snapshot> {
        match self.snapshot {
            Some(ref snapshot) => Ok(snapshot),
            None => throw!("not in a transaction")
        }
    }

    pub fn createColFamily(&self, columnFamilyName: &str) -> Result<()> {
        Ok(meta::STORE.dataStore.create_cf(columnFamilyName, &Options::default())?)
    }

    pub fn writeAddDataMutation(&self,
                                tableName: &String,
                                data: KV,
                                xmin: KV,
                                xmax: KV,
                                origin: KV) {
        let addMutation =
            Mutation::AddData {
                data,
                xmin,
                xmax,
                origin,
            };

        self.writeMutation(tableName, addMutation);
    }

    pub fn writeAddPointerMutation(&self,
                                   tableName: &String,
                                   xmin: KV, xmax: KV) {
        let addMutation =
            Mutation::AddPointer {
                xmin,
                xmax,
            };

        self.writeMutation(tableName, addMutation);
    }


    pub fn writeUpdateDataMutation(&self,
                                   tableName: &String,
                                   oldXmax: KV,
                                   newData: KV,
                                   newXmin: KV, newXmax: KV,
                                   origin: KV) {
        let updateMutation =
            Mutation::UpdateData {
                oldXmax,
                newData,
                newXmin,
                newXmax,
                origin,
            };

        self.writeMutation(tableName, updateMutation);
    }

    pub fn writeDeleteDataMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, Mutation::DeleteData { oldXmax })
    }

    pub fn writeDeletePointerMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, Mutation::DeletePointer { oldXmax })
    }

    fn writeMutation(&self, tableName: &String, mutation: Mutation) {
        let mut tableName_mutationsOnTable = self.tableName_mutations.borrow_mut();
        let mutationsOnTable = tableName_mutationsOnTable.getMutWithDefault(tableName);

        match mutation {
            Mutation::AddData { data, xmin, xmax, origin } => {
                mutationsOnTable.insert(data.0, data.1);
                mutationsOnTable.insert(xmin.0, xmin.1);
                mutationsOnTable.insert(xmax.0, xmax.1);
                mutationsOnTable.insert(origin.0, origin.1);
            }
            Mutation::UpdateData { oldXmax, newData, newXmin, newXmax, origin } => {
                mutationsOnTable.insert(oldXmax.0, oldXmax.1);
                mutationsOnTable.insert(newData.0, newData.1);
                mutationsOnTable.insert(newXmin.0, newXmin.1);
                mutationsOnTable.insert(newXmax.0, newXmax.1);
                mutationsOnTable.insert(origin.0, origin.1);
            }
            Mutation::DeleteData { oldXmax } => {
                mutationsOnTable.insert(oldXmax.0, oldXmax.1);
            }
            Mutation::DeletePointer { oldXmax } => {
                mutationsOnTable.insert(oldXmax.0, oldXmax.1);
            }
            Mutation::AddPointer { xmin, xmax } => {
                mutationsOnTable.insert(xmin.0, xmin.1);
                mutationsOnTable.insert(xmax.0, xmax.1);
            }
        };
    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            autoCommit: true,
            dataStore: &meta::STORE.dataStore,
            txId: None,
            tableName_mutations: Default::default(),
            snapshot: None,
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

pub enum Mutation {
    AddData {
        data: KV,
        xmin: KV,
        xmax: KV,
        origin: KV,
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
        origin: KV,
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
    pub tableName_tableMutation: HashMap<String, TableMutation>,
}

impl TxMutation {
    pub fn clear(&mut self) {
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