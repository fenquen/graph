use crate::cursor::Cursor;
use crate::db::DB;
use crate::types::TxId;
use crate::{constant, utils};
use anyhow::Result;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;

pub struct Tx {
    pub(crate) id: TxId,
    pub(crate) db: Arc<DB>,
    /// key without txId tail,no need <br>
    /// if val is None, means deletion
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(crate) committed: AtomicBool,
}

// pub fn
impl Tx {
    pub fn get(&self, keyWithoutTxId: &[u8]) -> Result<Option<Vec<u8>>> {
        // find in tx local changes
        if let Some(val) = self.changes.get(keyWithoutTxId) {
            return Ok(val.as_ref().map(|val| (&*val).clone()));
        }

        let keyWithTxId = self.appendKeyWithTxId(keyWithoutTxId);

        let scanMemTable =
            |memTable: &BTreeMap<Vec<u8>, Arc<Vec<u8>>>| -> Option<Arc<Vec<u8>>> {
                let memTableCurosr = memTable.upper_bound(Bound::Included(&keyWithTxId));

                if let Some((keyWithTxId0, val)) = memTableCurosr.peek_prev() {
                    let (originKey, txId) = parseKeyWithTxId(keyWithTxId0);

                    if keyWithoutTxId == originKey {
                        if txId <= self.id {
                            return Some(val.clone());
                        }
                    }
                }

                None
            };

        // find in memTables
        {
            let memTable = self.db.memTable.read().unwrap();

            if let Some(val) = scanMemTable(&*memTable) {
                return Ok(Some((&*val).clone()));
            }
        }

        // find in immutableMemTables in reverse order
        for immutableMemTable in self.db.immutableMemTables.iter().rev() {
            if let Some(val) = scanMemTable(immutableMemTable) {
                return Ok(Some((&*val).clone()));
            }
        }

        // find in lower level
        let mut cursor = self.createCursor()?;

        cursor.seek(keyWithoutTxId)?;

        if let Some((keyWithTxId, val)) = cursor.currentKV() {
            if keyWithTxId.starts_with(keyWithoutTxId) {
                let keyTxId = extractTxIdFromKeyWithTxId(keyWithTxId);
                if self.id >= keyTxId {
                    return Ok(Some(val.to_vec()));
                }
            }
        }

        Ok(None)
    }

    pub fn set(&mut self, keyWithoutTxId: &[u8], val: &[u8]) -> Result<()> {
        if keyWithoutTxId.is_empty() {
            throw!("key is empty");
        }

        if val.is_empty() {
            throw!("val is empty");
        }

        self.changes.insert(keyWithoutTxId.to_vec(), Some(val.to_vec()));

        Ok(())
    }

    pub fn delete(&mut self, keyWithoutTxId: &[u8]) -> Result<()> {
        if keyWithoutTxId.is_empty() {
            throw!("key is empty");
        }

        self.changes.insert(keyWithoutTxId.to_vec(), None);

        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        if let Err(_) = self.committed.compare_exchange(false, true,
                                                        Ordering::SeqCst, Ordering::Acquire) {
            return Ok(());
        }

        if self.changes.is_empty() {
            return Ok(());
        }

        let commitReqSender = self.db.commitReqSender.clone();

        let (commitResultSender, commitResultReceiver) =
            mpsc::sync_channel::<Result<()>>(1);

        let commitReq = CommitReq {
            txId: self.id,
            changes: self.changes,
            commitResultSender,
        };

        commitReqSender.send(commitReq)?;

        for commitResult in commitResultReceiver {
            commitResult?;
        }

        Ok(())
    }
}

// pub (crate) fn
impl Tx {
    pub(crate) fn appendKeyWithTxId(&self, key: &[u8]) -> Vec<u8> {
        let mut keyWithTxId = Vec::with_capacity(key.len() + constant::TX_ID_SIZE);
        keyWithTxId.extend_from_slice(&key[..]);
        keyWithTxId.extend_from_slice(self.id.to_be_bytes().as_ref());
        keyWithTxId
    }
}

// fn
impl Tx {
    fn createCursor(&self) -> Result<Cursor> {
        Cursor::new(self)
    }
}

fn parseKeyWithTxId(keyWithTxId: &[u8]) -> (&[u8], TxId) {
    let pos = keyWithTxId.len() - constant::TX_ID_SIZE;
    let originKey = &keyWithTxId[0..pos];
    let txId = utils::slice2ArrayRef(&keyWithTxId[pos..]).unwrap();
    (originKey, TxId::from_be_bytes(*txId))
}

fn extractTxIdFromKeyWithTxId(keyWithTxId: &[u8]) -> TxId {
    let pos = keyWithTxId.len() - constant::TX_ID_SIZE;
    let txId = utils::slice2ArrayRef(&keyWithTxId[pos..]).unwrap();

    TxId::from_be_bytes(*txId)
}

pub(crate) struct CommitReq {
    pub(crate) txId: TxId,
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(crate) commitResultSender: SyncSender<Result<()>>,
}