use std::sync::atomic::Ordering;
use crate::{command_executor, global, parser};
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

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub async fn exexute(&mut self, sql: &str) -> Result<()> {
        if self.autoCommit || self.txId == global::TX_ID_INVALID {
            self.txId = global::TX_ID.fetch_add(1, Ordering::SeqCst);
        }

        let commandVec = parser::parse(sql.as_str())?;
        command_executor::execute(commandVec).await?;
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