use std::sync::atomic::Ordering;
use crate::{meta, throw, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta::{DBObject, Index, Table};
use anyhow::Result;
use crate::utils::HashMapExt;

impl<'session> CommandExecutor<'session> {
    pub(super) fn createTable(&self, mut table: Table, isTable: bool) -> Result<CommandExecResult> {
        if meta::NAME_DB_OBJ.contains_key(table.name.as_str()) {
            if table.createIfNotExist == false {
                throw!(&format!("table/relation: {} already exist", table.name))
            }

            return Ok(CommandExecResult::DdlResult);
        }

        table.id = meta::DB_OBJECT_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // 生成column family
        self.session.createColFamily(table.name.as_str())?;

        // todo 使用 u64的tableId 为key 完成
        let key = u64ToByteArrRef!(table.id);

        let dbObject =
            if isTable == false {
                DBObject::Relation(table)
            } else {
                DBObject::Table(table)
            };

        meta::STORE.metaStore.put(key, serde_json::to_string(&dbObject)?.as_bytes())?;

        // map
        meta::NAME_DB_OBJ.insert(dbObject.getName(), dbObject);

        Ok(CommandExecResult::DdlResult)
    }

    // todo 对非unique的index如何应对重复的value
    pub(super) fn createIndex(&self, mut index: Index) -> Result<CommandExecResult> {
        if meta::NAME_DB_OBJ.contains_key(index.name.as_str()) {
            if index.createIfNotExist == false {
                throw!(&format!("index: {} already exist", index.name))
            }

            return Ok(CommandExecResult::DdlResult);
        }

        // 需要对index涉及的table和column校验的
        let targetTable = meta::NAME_DB_OBJ.get(index.tableName.as_str());
        if targetTable.is_none() {
            throw!(&format!("create index failed , target table {} not exist", index.tableName ));
        }
        let targetTable = targetTable.unwrap();
        // 隐含了index的对象需要是table
        let targetTable = targetTable.asTable()?;

        let tableColumnNames: Vec<&str> = targetTable.columns.iter().map(|tableColumn| tableColumn.name.as_str()).collect();

        for targetColumnName in &index.columnNames {
            if tableColumnNames.contains(&targetColumnName.as_str()) == false {
                throw!(&format!("table: {} does not contain column: {}", index.tableName ,targetColumnName));
            }
        }

        index.id = meta::DB_OBJECT_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // todo 如何知道表涉及到的index有哪些
        // 建立table和相应的index的联系
        {
            let mut tableName_indexNames = meta::TABLE_NAME_INDEX_NAMES.write().unwrap();
            let indexNames = tableName_indexNames.getMutWithDefault(&index.tableName);
            indexNames.push(index.tableName.clone());
        }

        // 生成column family
        self.session.createColFamily(index.name.as_str())?;

        let key = u64ToByteArrRef!(index.id);

        let dbObject = DBObject::Index(index);

        meta::STORE.metaStore.put(key, serde_json::to_string(&dbObject)?.as_bytes())?;
        meta::NAME_DB_OBJ.insert(dbObject.getName(), dbObject);

        Ok(CommandExecResult::DdlResult)
    }
}