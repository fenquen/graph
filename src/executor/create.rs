use std::sync::atomic::Ordering;
use crate::{meta, throw, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta::Table;

impl<'session> CommandExecutor<'session> {
    pub(super) fn createTable(&self, mut table: Table) -> anyhow::Result<CommandExecResult> {
        if meta::TABLE_NAME_TABLE.contains_key(table.name.as_str()) {
            if table.createIfNotExist == false {
                throw!(&format!("table/relation: {} already exist", table.name))
            }

            return Ok(CommandExecResult::DdlResult);
        }

        table.tableId = meta::TABLE_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // 生成column family
        self.session.createColFamily(table.name.as_str())?;

        // todo 使用 u64的tableId 为key 完成
        let key = u64ToByteArrRef!(table.tableId);
        let json = serde_json::to_string(&table)?;
        meta::STORE.metaStore.put(key, json.as_bytes())?;

        // map
        meta::TABLE_NAME_TABLE.insert(table.name.to_string(), table);

        Ok(CommandExecResult::DdlResult)
    }
}