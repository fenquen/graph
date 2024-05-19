use std::sync::Arc;
use std::sync::atomic::Ordering;
use crate::{command_executor, global, meta, parser, throw};
use anyhow::Result;
use rocksdb::{BoundColumnFamily, OptimisticTransactionDB, Options, Transaction};
use tokio::io::AsyncWriteExt;
use crate::command_executor::CommandExecutor;
use crate::parser::{Command, SqlOp};

pub struct Session<'db> {
    autoCommit: bool,
    currentTx: Option<Transaction<'db, OptimisticTransactionDB>>,
}

impl<'db> Session<'db> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn setAutoCommit(&mut self, autoCommit: bool) {
        self.autoCommit = autoCommit;
    }

    /// 如果是autoCommit 该调用是个tx 可能传递的&str包含了多个独立的sql
    pub async fn executeSql(&mut self, sql: &str) -> Result<()> {
        let mut commands = parser::parse(sql)?;

        if commands.is_empty() {
            return Ok(());
        }

        self.executeCommands(&mut commands).await
    }

    async fn executeCommands(&mut self, commands: &mut [Command]) -> Result<()> {
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
    pub fn getCurrentTx(&self) -> Result<&Transaction<'db, OptimisticTransactionDB>> {
        match self.currentTx.as_ref() {
            Some(tx) => Ok(tx),
            None => throw!("not in a transaction")
        }
    }

    pub fn getColFamily(&self, colFamilyName: &str) -> Result<Arc<BoundColumnFamily>> {
        match meta::STORE.data.cf_handle(colFamilyName) {
            Some(cf) => Ok(cf),
            None => throw!(&format!("column family:{} not exist", colFamilyName))
        }
    }

    pub fn createColFamily(&self, columnFamilyName: &str) -> Result<()> {
        Ok(meta::STORE.data.create_cf(columnFamilyName, &Options::default())?)
    }
}

impl Default for Session<'_> {
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
        let a: Object = serde_json::from_str("").unwrap();
    }
}