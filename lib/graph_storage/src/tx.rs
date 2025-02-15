use std::collections::BTreeMap;
use std::sync::Arc;
use crate::db::DB;
use crate::types::TxId;

pub(crate) struct Tx {
    pub(crate) id: TxId,
    pub(crate) writable: bool,
    pub(crate) db: Arc<DB>,
    pub(crate) changes: BTreeMap<Arc<Vec<u8>>, Entry>,
}

pub(crate) struct Entry {
    pub(crate) key: Arc<Vec<u8>>,

    /// none when delete key
    pub(crate) value: Option<Vec<u8>>,

    pub(crate) txId: TxId,

    ///  if false the key(Vec<u8>) is actually shared from somewhere else
    pub(crate) keyIsExclusive: bool,
}

impl Entry {
    // key is exclusivelly owned by self
    pub(crate) fn newExclusive(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        panic!()
    }
}
