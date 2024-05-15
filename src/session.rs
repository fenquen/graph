use std::sync::Arc;
use std::sync::atomic::Ordering;
use crate::{command_executor, global, meta, parser, throw};
use anyhow::Result;
use rocksdb::{BoundColumnFamily, OptimisticTransactionDB, Transaction};
use tokio::io::AsyncWriteExt;
use crate::command_executor::CommandExecutor;
use crate::global::{DataLen, TX_ID_COUNTER, TxId};
use crate::parser::{Command, SqlOp};

pub struct Session {
    autoCommit: bool,
    currentTx: Option<Transaction<'static, OptimisticTransactionDB>>,
}

impl Session {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn setAutoCommit(&mut self, autoCommit: bool) {
        self.autoCommit = autoCommit;
    }

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub async fn executeSql(&mut self, sql: &str) -> Result<()> {
        let commands = parser::parse(sql)?;
        self.executeCommands(&commands).await
    }

    pub async fn executeCommands(&mut self, commands: &[Command]) -> Result<()> {
        self.currentTx = Some(meta::STORE.data.transaction());

        let commandExecutor = CommandExecutor::new(self);
        commandExecutor.execute(commands).await?;

        if self.autoCommit {
            self.currentTx.take().unwrap().commit()?;
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.currentTx.take().unwrap().commit()?;
        Ok(())
    }

    #[inline]
    pub fn getCurrentTx(&self) -> Result<&Transaction<'static, OptimisticTransactionDB>> {
        match self.currentTx.as_ref() {
            Some(tx) => Ok(tx),
            None => throw!("not in a transaction")
        }
    }

    pub fn getColFamily(&self, colFamilyName: &str) -> Result<Arc<BoundColumnFamily>> {
        match meta::STORE.data.cf_handle(colFamilyName) {
            Some(cf) => Ok(cf),
            None => throw!(&format!("column family:{} not exist",colFamilyName))
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            autoCommit: true,
            currentTx: None,
        }
    }
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
        let a: Object = serde_json::from_str(r#"{"name":"a","next":{"name":"b","next":null}}"#).unwrap();
    }
}