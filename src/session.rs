use std::alloc::Allocator;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{BTreeMap};
use std::hash::Hash;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use crate::{config, global, meta, parser, throw, throwFormat, u64ToByteArrRef};
use anyhow::Result;
use bumpalo::Bump;
use dashmap::mapref::one::{Ref, RefMut};
use hashbrown::{HashMap, HashSet};
use bytes::BytesMut;
use log::log;
use rocksdb::{BoundColumnFamily, DB, DBAccess, SnapshotWithThreadMode, DBWithThreadMode};
use rocksdb::{MultiThreaded, OptimisticTransactionDB, Options, Transaction, WriteBatchWithTransaction};
use tokio::io::AsyncWriteExt;
use graph_independent::AllocatorExt;
use crate::config::{Config, CONFIG};
use crate::executor::CommandExecutor;
use crate::meta::DBObject;
use crate::parser::command::Command;
use crate::types::{Byte, ColumnFamily, DBObjectId, DBRawIterator, KV, SelectResultToFront, SessionHashMap, SessionHashSet, SessionVec, Snapshot, TableMutations, TxId};
use crate::utils::HashMapExt;

pub struct Session {
    autoCommit: bool,
    txId: Option<TxId>,
    dataStore: &'static DB,
    metaStore: &'static DB,
    pub tableName_mutationsOnTable: RwLock<HashMap<String, TableMutations>>,
    snapshot: Option<Snapshot<'static>>,
    pub scanConcurrency: usize,
    pub bump: Bump,
}

impl Session {
    pub fn new() -> Self {
        Session::default()
    }

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub fn executeSql(&mut self, sql: &str) -> Result<SelectResultToFront> {
        let mut commands = parser::parse(sql)?;

        if commands.is_empty() {
            return Ok(Vec::new());
        }

        // todo set autocommit 是不需要tx的 如果sql只包含set如何应对
        let mut needTx = false;
        for command in &commands {
            if command.needTx() {
                needTx = true;
                break;
            }
        }

        // 不要求是不是auto commit的 要不在tx那么生成1个tx
        if self.notInTx() && needTx {
            self.generateTx()?;
        }

        // todo 要是执行的过程有报错 是不是应该rollback
        let selectResultToFront = CommandExecutor::new(self).execute(&mut commands)?;

        // todo sql中执行了commit rollback使得当前tx提交后,当前不是inTx了,要是后边还有不是set的sql需要再重起1个tx
        if self.autoCommit && needTx {
            self.commit()?;
        }

        Ok(selectResultToFront)
    }

    /// 提交之后 在到下个执行sql前 session都是 not in tx 的
    pub fn commit(&mut self) -> Result<()> {
        // todo sql中执行了commit导致当前tx提交后,当前不是inTx了,调用commit报错,需要commit()不要限制inTx 完成
        if self.notInTx() {
            return Ok(self.clean());
        }

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for (tableName, mutations) in self.tableName_mutationsOnTable.read().unwrap().iter() {
            let colFamily = Session::getColumnFamily(tableName)?;
            for (key, value) in mutations {
                batch.put_cf(&colFamily, key, value);
            }
        }

        // todo lastest的txId要持久化 完成
        {
            let currentTxId = self.txId.unwrap();
            // cf需要现用现取 内部使用的是read 而create cf会用到write
            let cf = Session::getColumnFamily(meta::COLUMN_FAMILY_NAME_TX_ID)?;

            let txUndergoingMaxCount = CONFIG.txUndergoingMaxCount.load(Ordering::Acquire) as u64;

            if (currentTxId - *meta::TX_ID_START_UP.getRef()) % txUndergoingMaxCount == 0 {
                // TX_CONCURRENCY_MAX
                // tokio::task::spawn_blocking(move || {
                let thresholdTx = currentTxId - txUndergoingMaxCount;
                CommandExecutor::vaccumData(thresholdTx);
                // });

                // todo 干掉columnFamily "tx_id" 老的txId 完成
                self.dataStore.delete_range_cf(&cf, u64ToByteArrRef!(meta::TX_ID_INVALID), u64ToByteArrRef!(currentTxId))?;
            }

            // 以当前的txId为key落地到单独的columnFamil "tx_id"
            batch.put_cf(&cf, u64ToByteArrRef!(self.txId.unwrap()), global::EMPTY_BINARY);
        }

        self.dataStore.write(batch)?;

        meta::TX_UNDERGOING_COUNT.fetch_sub(1, Ordering::AcqRel);

        Ok(self.clean())
    }

    // todo rollback()不要求inTx 完成
    pub fn rollback(&mut self) -> Result<()> {
        if self.notInTx() == false {
            self.clean();
        }

        Ok(())
    }

    fn clean(&mut self) {
        self.txId = None;
        self.snapshot = None;
        self.tableName_mutationsOnTable.write().unwrap().clear();
        self.bump.reset();
    }

    pub fn generateTx(&mut self) -> Result<()> {
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
        self.tableName_mutationsOnTable.write().unwrap().clear();

        Ok(())
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

    pub fn setScanConcurrency(&mut self, scanConcurrency: usize) -> Result<()> {
        self.scanConcurrency = scanConcurrency;
        Ok(())
    }

    pub fn getTxId(&self) -> Result<TxId> {
        match self.txId {
            Some(txId) => Ok(txId),
            None => throw!("not in a transaction")
        }
    }

    pub fn getColumnFamily(columnFamilyName: &str) -> Result<ColumnFamily> {
        match meta::STORE.dataStore.cf_handle(columnFamilyName) {
            Some(cf) => Ok(cf),
            None => throwFormat!("column family:{} not exist", columnFamilyName)
        }
    }

    pub fn getSnapshot(&self) -> Result<&Snapshot> {
        match self.snapshot {
            Some(ref snapshot) => Ok(snapshot),
            None => throw!("not in a transaction")
        }
    }

    #[inline]
    pub fn getDBRawIterator(&self, columnFamily: &ColumnFamily) -> Result<DBRawIterator> {
        Ok(self.getSnapshot()?.raw_iterator_cf(columnFamily))
    }

    #[inline]
    pub fn getDBRawIteratorWithoutSnapshot(&self, columnFamily: &ColumnFamily) -> Result<DBRawIterator> {
        Ok(self.dataStore.raw_iterator_cf(columnFamily))
    }

    #[inline]
    pub fn createColFamily(&self, name: &str) -> Result<()> {
        Ok(self.dataStore.create_cf(name, &Options::default())?)
    }

    #[inline]
    pub fn putUpdateMeta(&self, dbObjectId: DBObjectId, dbObject: &DBObject) -> Result<()> {
        let key = &dbObjectId.to_be_bytes()[..];
        Ok(self.metaStore.put(key, serde_json::to_string(dbObject)?.as_bytes())?)
    }

    #[inline]
    pub fn dropColFamily(&self, name: &str) -> Result<()> {
        Ok(self.dataStore.drop_cf(name)?)
    }

    #[inline]
    pub fn deleteMeta(&self, dbObjectId: DBObjectId) -> Result<()> {
        let key = &dbObjectId.to_be_bytes()[..];
        Ok(self.metaStore.delete(key)?)
    }

    /// 直接上手datastore
    #[inline]
    pub fn deleteUnderCf(&self, key: &[Byte], columnFamily: &ColumnFamily) -> Result<()> {
        Ok(self.dataStore.delete_cf(columnFamily, key)?)
    }

    #[inline]
    pub fn deleteRangeUnderCf(&self, columnFamily: &ColumnFamily, from: &[Byte], to: &[Byte]) -> Result<()> {
        Ok(self.dataStore.delete_range_cf(columnFamily, from, to)?)
    }

    // todo getDBObjectByName用不到self迁移到session 完成
    pub fn getDBObjectByName(dbObjectName: &str) -> Result<Ref<String, DBObject>> {
        match meta::NAME_DB_OBJ.get(dbObjectName) {
            None => throwFormat!("db object:{} not exist", dbObjectName),
            Some(dbObject) => {
                if dbObject.invalid() {
                    throwFormat!("db object:{} not exist", dbObjectName);
                }

                Ok(dbObject)
            }
        }
    }

    /// 可以起到独占锁的效果
    pub fn getDBObjectMutByName(dbObjectName: &str) -> Result<RefMut<String, DBObject>> {
        match meta::NAME_DB_OBJ.get_mut(dbObjectName) {
            None => throwFormat!("db object:{} not exist", dbObjectName),
            Some(dbObject) => {
                if dbObject.invalid() {
                    throwFormat!("db object:{} not exist", dbObjectName);
                }

                Ok(dbObject)
            }
        }
    }

    pub fn writeAddDataMutation(&self,
                                tableName: &String,
                                data: KV,
                                xmin: KV, xmax: KV,
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

    pub fn writeAddDataMutation2Dest<'a>(&self,
                                         tableName: &'a String,
                                         data: KV,
                                         xmin: KV, xmax: KV,
                                         origin: KV, dest: &mut SessionVec<(&'a String, Mutation)>) {
        let addData =
            Mutation::AddData {
                data,
                xmin,
                xmax,
                origin,
            };

        dest.push((tableName, addData))
    }

    pub fn writeAddPointerMutation(&self,
                                   tableName: &String,
                                   xmin: KV, xmax: KV) {
        let addPointer = Mutation::AddPointer { xmin, xmax };
        self.writeMutation(tableName, addPointer);
    }

    pub fn writeAddPointerMutation2Dest<'a>(&self,
                                            tableName: &'a String,
                                            xmin: KV, xmax: KV,
                                            dest: &mut SessionVec<(&'a String, Mutation)>) {
        let addPointer =
            Mutation::AddPointer {
                xmin,
                xmax,
            };

        dest.push((tableName, addPointer))
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

    #[inline]
    pub fn writeDeleteDataMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, Mutation::DeleteData { oldXmax })
    }

    #[inline]
    pub fn writeDeletePointerMutation(&self, tableName: &String, oldXmax: KV) {
        self.writeMutation(tableName, Mutation::DeletePointer { oldXmax })
    }

    #[inline]
    pub fn writeAddIndexMutation(&self, indexName: &String, data: KV) {
        self.writeMutation(indexName, Mutation::AddIndex { data })
    }

    pub fn writeMutation(&self, dbObjectName: &String, mutation: Mutation) {
        let mut tableName_mutationsOnTable = self.tableName_mutationsOnTable.write().unwrap();
        let mutationsOnTable = tableName_mutationsOnTable.getMutWithDefault(dbObjectName);

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
            Mutation::AddPointer { xmin, xmax } => {
                mutationsOnTable.insert(xmin.0, xmin.1);
                mutationsOnTable.insert(xmax.0, xmax.1);
            }
            Mutation::DeletePointer { oldXmax } => {
                mutationsOnTable.insert(oldXmax.0, oldXmax.1);
            }
            Mutation::AddIndex { data } => {
                mutationsOnTable.insert(data.0, data.1);
            }
        };
    }

    #[inline]
    pub fn withCapacityIn(&self, capacity: usize) -> BytesMut {
        BytesMut::with_capacity_in(capacity, &self.bump)
    }

    #[inline]
    pub fn vecWithCapacityIn<T>(&self, capacity: usize) -> SessionVec<T> {
        Vec::with_capacity_in(capacity, &self.bump)
    }

    #[inline]
    pub fn hashMapWithCapacityIn<K, V>(&self, capacity: usize) -> SessionHashMap<K, V> {
        HashMap::with_capacity_in(capacity, &self.bump)
    }

    #[inline]
    pub fn hashSetWithCapacityIn<T: Hash + Eq>(&self, capacity: usize) -> SessionHashSet<T> {
        HashSet::with_capacity_in(capacity, &self.bump)
    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            autoCommit: true,
            dataStore: &meta::STORE.dataStore,
            metaStore: &meta::STORE.metaStore,
            txId: None,
            tableName_mutationsOnTable: Default::default(),
            snapshot: None,
            scanConcurrency: 1,
            bump: Bump::with_capacity(config::CONFIG.sessionMemotySize),
        }
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
    AddIndex {
        data: KV
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

#[cfg(test)]
mod test {
    use bumpalo::Bump;
    use bytes::BytesMut;
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
        serde_json::from_str::<Object>("").unwrap();
    }

    #[test]
    pub fn testBump() {
        let mut bump = Bump::with_capacity(1000000);

        let mut v0 = Vec::<u8, &Bump>::with_capacity_in(1000000, &bump);
        v0.push(0);
        println!("{}", bump.allocated_bytes());

        let mut v1 = Vec::<u8, &Bump>::new_in(&bump);
        v1.push(1);
        println!("{}", bump.allocated_bytes());


        v0.push(7);

        println!("{}", v0[0]);

        //bump.reset();
        //println!("{}", bump.allocated_bytes());

        /*bump.alloc(1u64);
        println!("{}", bump.allocated_bytes());
        bump.alloc(1u8);
        println!("{}", bump.allocated_bytes());*/
    }
}