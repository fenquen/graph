use crate::cursor::Cursor;
use crate::db::DB;
use crate::mem_table::MemTable;
use crate::types::TxId;
use crate::{constant, utils};
use anyhow::Result;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::mpsc;

pub struct Tx<'db> {
    pub(crate) id: TxId,
    pub(crate) db: &'db DB,
    /// key without txId tail,no need <br>
    /// if val is None, means deletion
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(crate) committed: AtomicBool,
}

impl<'db> Tx<'db> {
    pub fn get(&self, targetKeyWithoutTxId: &[u8]) -> Result<Option<Vec<u8>>> {
        // find in tx local changes
        if let Some(val) = self.changes.get(targetKeyWithoutTxId) {
            return Ok(val.as_ref().map(|val| (&*val).clone()));
        }

        let keyWithTxId = appendKeyWithTxId(targetKeyWithoutTxId, self.id);

        // 返回的需要有1个单个的标识来表明是不是exist
        // 单单的none是不能区分的,因为none可能是真的没有也可能是有只不过value是None
        let scanMemTable =
            |memTable: &MemTable| -> (Option<Vec<u8>>, bool) {
                let memTableCursor = memTable.changes.upper_bound(Bound::Included(&keyWithTxId));

                if let Some((keyWithTxId0, value)) = memTableCursor.peek_prev() {
                    let (originKey, txId) = parseKeyWithTxId(keyWithTxId0);

                    if targetKeyWithoutTxId == originKey {
                        if txId <= self.id {
                            return (value.clone(), true);
                        }
                    }
                }

                (None, false)
            };

        // find in memTables
        // 需要区分是真的没有,还是说有只不过value是None
        {
            let memTable = self.db.memTable.read().unwrap();

            if let (value, true) = scanMemTable(&*memTable) {
                return Ok(value);
            }
        }

        // find in immutableMemTables in reverse order
        // 需要区分是真的没有,还是说有只不过value是None
        {
            let immutableMemTables = self.db.immutableMemTables.read().unwrap();

            for immutableMemTable in immutableMemTables.iter().rev() {
                if let (value, true) = scanMemTable(immutableMemTable) {
                    return Ok(value);
                }
            }
        }

        // find in lower level
        let mut cursor = self.createCursor()?;

        cursor.seek(targetKeyWithoutTxId, None, false, 0)?;

        if let Some((keyWithTxId, value)) = cursor.currentKV() {
            let (keyWithoutTxId, _) = parseKeyWithTxId(keyWithTxId.as_slice());

            if keyWithoutTxId == targetKeyWithoutTxId {
                let (_, keyTxId) = parseKeyWithTxId(keyWithTxId.as_slice());

                if self.id >= keyTxId {
                    return Ok(value);
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
        // already committed
        if let Err(_) = self.committed.compare_exchange(false, true,
                                                        Ordering::SeqCst, Ordering::Acquire) {
            return Ok(());
        }

        if self.changes.is_empty() {
            return Ok(());
        }

        let (commitResultSender, commitResultReceiver) =
            mpsc::sync_channel::<Result<()>>(1);

        let commitReq = CommitReq {
            txId: self.id,
            changes: self.changes,
            commitResultSender,
        };

        self.db.commitReqSender.send(commitReq)?;

        for commitResult in commitResultReceiver {
            commitResult?;
        }

        {
            let mut infightingTxIds = self.db.flyingTxIds.write().unwrap();
            infightingTxIds.remove(&self.id);
        }

        Ok(())
    }

    fn createCursor(&'_ self) -> Result<Cursor<'_, '_>> {
        Cursor::new(&*self.db, Some(self))
    }
}

pub(crate) fn parseKeyWithTxId(keyWithTxId: &[u8]) -> (&[u8], TxId) {
    let pos = keyWithTxId.len() - constant::TX_ID_SIZE;
    let originKey = &keyWithTxId[0..pos];
    let txId = utils::slice2ArrayRef(&keyWithTxId[pos..]).unwrap();
    (originKey, TxId::from_be_bytes(*txId))
}

pub(crate) fn getKeyFromKeyWithTxId(keyWithTxId: &[u8]) -> &[u8] {
    let (key, _) = parseKeyWithTxId(keyWithTxId);
    key
}

pub(crate) fn appendKeyWithTxId(key: &[u8], txId: TxId) -> Vec<u8> {
    let mut keyWithTxId = Vec::with_capacity(key.len() + constant::TX_ID_SIZE);
    keyWithTxId.extend_from_slice(&key[..]);
    keyWithTxId.extend_from_slice(txId.to_be_bytes().as_ref());
    keyWithTxId
}

pub(crate) fn appendKeyWithTxId0(mut key: Vec<u8>, txId: TxId) -> Vec<u8> {
    key.extend_from_slice(txId.to_be_bytes().as_ref());
    key
}

pub(crate) struct CommitReq {
    pub(crate) txId: TxId,
    pub(crate) changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(crate) commitResultSender: SyncSender<Result<()>>,
}