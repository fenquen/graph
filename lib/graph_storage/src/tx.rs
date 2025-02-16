use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use crate::constant;
use crate::db::DB;
use crate::types::TxId;

pub(crate) struct Tx {
    pub(crate) id: TxId,
    pub(crate) writable: bool,
    pub(crate) db: Arc<DB>,
    /// key without txId tail,no need
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Arc<Vec<u8>>>>,
}

// pub fn
impl Tx {
    pub fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        // find in tx local changes
        if let Some(val) = self.changes.get(key) {
            return val.as_ref().map(|val| val.clone());
        }

        // find in memTables
        let keyWithTxId = self.appendKeyWithTxId(key);
        keyWithTxId.as_slice();

        let memTableCurosr = self.db.memTable.upper_bound(Bound::Included(&keyWithTxId));
        if let Some((keyWithTxId0, val)) = memTableCurosr.peek_prev() {
            let (originKey, txId) = parseKeyWithTxId(keyWithTxId0);

            if key == originKey {}
        }

        // find in lower

        None
    }
}

impl Tx {
    fn appendKeyWithTxId(&self, key: &[u8]) -> Vec<u8> {
        let mut keyWithTxId = Vec::with_capacity(key.len() + constant::TX_ID_SIZE);
        keyWithTxId.copy_from_slice(&key[..]);
        keyWithTxId.copy_from_slice(self.id.to_be_bytes().as_ref());
        keyWithTxId
    }
}

fn parseKeyWithTxId(keyWithTxId: &[u8]) -> (&[u8], &[u8]) {
    let pos = keyWithTxId.len() - constant::TX_ID_SIZE;
    (&keyWithTxId[0..pos], &keyWithTxId[pos..])
}