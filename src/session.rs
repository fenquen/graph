use std::sync::atomic::Ordering;
use crate::{executor, global, parser};
use anyhow::Result;
use crate::global::{TX_ID, TxId};

pub struct Session {
    autoCommit: bool,
    txId: TxId,
}

impl Session {
    pub fn setAutoCommit(&mut self, autoCommit: bool) {
        self.autoCommit = autoCommit;
    }

    pub async fn exexute(&mut self, sql: &str) -> Result<()> {
        if self.autoCommit || self.txId == global::TX_ID_INVALID {
            self.txId = global::TX_ID.fetch_add(1, Ordering::SeqCst);
        }

        let commandVec = parser::parse(sql.as_str())?;
        executor::execute(commandVec).await?;
    }

    pub fn commit(&mut self) {
        let txId = self.txId;

        if self.autoCommit == false {
            self.txId = global::TX_ID_INVALID;
        }


    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            autoCommit: true,
            txId: global::TX_ID_INVALID,
        }
    }
}