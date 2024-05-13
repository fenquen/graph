use std::sync::atomic::Ordering;
use crate::{command_executor, global, parser};
use anyhow::Result;
use tokio::io::AsyncWriteExt;
use crate::command_executor::CommandExecutor;
use crate::global::{DataLen, TX_ID_COUNTER, TxId};
use crate::parser::{Command, SqlOp};

pub struct Session {
    pub autoCommit: bool,
    pub txId: TxId,
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
        if self.autoCommit || self.txId == global::TX_ID_INVALID {
            self.txId = global::TX_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        }

        let commandExecutor = CommandExecutor::new(self);
        commandExecutor.execute(commands).await?;

        if self.autoCommit {
            self.commit(commands).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, commands: &[Command]) -> Result<()> {
        let txId = self.txId;

        // 手动
        if self.autoCommit == false {
            self.txId = global::TX_ID_INVALID;
        }

        let mut collect = Vec::with_capacity(commands.len());

        for command in commands {
            if command.isDml() == false {
                continue;
            }

            collect.push(command);
        }

        self.writeWal(&collect, txId).await?;

        Ok(())
    }

    /// commandVec里的dml的json string
    /// 8byte txId + 4byte content length + content + 8byte txId
    pub async fn writeWal(&self, dmlCommands: &[&Command], txId: TxId) -> Result<()> {
        if dmlCommands.is_empty() {
            return Ok(());
        }

        // 第1个deref得到的是Arc<Option<RwLock<File>>>
        // 第2个deref得到的是Option<RwLock<File>>
        let option = &(**global::WAL_FILE.load());
        let mut walFile = option.as_ref().unwrap().write().await;

        let jsonString = serde_json::to_string(dmlCommands)?;
        let jsonStringByte = jsonString.as_bytes();

        // 打头的tx_id
        walFile.write_u64(txId).await?;

        // content length
        walFile.write_u32(jsonStringByte.len() as DataLen).await?;

        // content
        walFile.write_all(jsonStringByte).await?;

        // 收尾的tx_id
        walFile.write_u64(txId).await?;

        walFile.sync_data().await?;

        // walFile.write()


        Ok(())
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