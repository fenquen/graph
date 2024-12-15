use std::alloc::{Allocator, Global};
use std::collections::{BTreeMap};
use std::ops::Bound;
use std::sync::Arc;
use bumpalo::Bump;
use hashbrown::DefaultHashBuilder;
use hashbrown::{HashMap, HashSet};
use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, DBRawIteratorWithThreadMode};
use rocksdb::{DBWithThreadMode, MultiThreaded, SnapshotWithThreadMode};
use serde_json::Value;
use graph_independent::AllocatorExt;
use crate::executor::{CommandExecutor, IterationCmd};
use crate::graph_value::GraphValue;
use anyhow::Result;

/// 到后台的sql可能是由多个小sql构成的 单个小select的sql对应个Vec<Value>
pub type SelectResultToFront = Vec<Vec<Value>>;

pub type DBRawIterator<'db> = DBRawIteratorWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;
pub type DBIterator<'db> = DBIteratorWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;

pub type ColumnFamily<'db> = Arc<BoundColumnFamily<'db>>;

pub type Snapshot<'db> = SnapshotWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>;

pub type Byte = u8;

pub type DBObjectId = u64;
pub type RowId = u64;

pub type KeyPrefix = Byte;
pub type DataKey = u64;
pub type KeyTag = Byte;

pub type TxId = u64;

pub type KV = (Vec<Byte>, Vec<Byte>);

pub trait CommittedPreProcessor = FnMut(&ColumnFamily, DataKey) -> Result<bool>;
pub trait CommittedPostProcessor = FnMut(&ColumnFamily, DataKey, &RowData) -> Result<bool>;
pub trait UncommittedPreProcessor = FnMut(&TableMutations, DataKey) -> Result<bool>;
pub trait UncommittedPostProcessor = FnMut(&TableMutations, DataKey, &RowData) -> Result<bool>;

/// columnFamily committedPointerKey(会随着变化) prefix(不会变化)
pub trait CommittedPointerKeyProcessor = FnMut(&ColumnFamily, &[Byte], &[Byte]) -> Result<IterationCmd>;
/// tableMutations addedPointerKey(会随着变化) prefix(不会变化)
pub trait UncommittedPointerKeyProcessor = FnMut(&TableMutations, &[Byte], &[Byte]) -> Result<IterationCmd>;

pub type TableMutations = BTreeMap<Vec<Byte>, Vec<Byte>>;

/// 起点只会是indclude 终点只会是include unbound
pub type RelationDepth = (Bound<usize>, Bound<usize>);

pub type Pointer = u64;

pub type RowData<A = Global> = HashMap<String, GraphValue, DefaultHashBuilder, A>;

pub trait HashMapExt {
    fn getRowSize(&self) -> usize;
}

impl<A: Allocator> HashMapExt for HashMap<String, GraphValue, DefaultHashBuilder, A> {
    fn getRowSize(&self) -> usize {
        if self.dummy {
            return 0;
        }
        
        let mut size: usize = 0;

        self.values().for_each(|graphValue| {
            if let Some(s) = graphValue.size() {
                size += s;
            }
        });

        size
    }
}

pub type SessionVec<'a, T> = Vec<T, &'a Bump>;
pub type SessionHashMap<'a, K, V> = HashMap<K, V, DefaultHashBuilder, &'a Bump>;
pub type SessionHashSet<'a, T> = HashSet<T, DefaultHashBuilder, &'a Bump>;

pub type ElementType = u8;