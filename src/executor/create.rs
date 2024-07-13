use std::sync::atomic::Ordering;
use crate::{getKeyIfSome, global, meta, throw, throwFormat, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta::{DBObject, Index, Table};
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use crate::codec::BinaryCodec;
use crate::executor::store::ScanParams;
use crate::session::Session;
use crate::types::DBRawIterator;
use crate::utils::HashMapExt;

impl<'session> CommandExecutor<'session> {
    pub(super) fn createTable(&self, mut table: Table, isTable: bool) -> Result<CommandExecResult> {
        if meta::NAME_DB_OBJ.contains_key(table.name.as_str()) {
            if table.createIfNotExist == false {
                throwFormat!("table/relation: {} already exist", table.name);
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

    // todo 对非unique的index如何应对重复的value,后边添加dataKey 完成
    pub(super) fn createIndex(&self, mut index: Index) -> Result<CommandExecResult> {
        if meta::NAME_DB_OBJ.contains_key(index.name.as_str()) {
            if index.createIfNotExist == false {
                throwFormat!("index: {} already exist", index.name);
            }

            return Ok(CommandExecResult::DdlResult);
        }

        // 需要对index涉及的table和column校验的
        let dbObjectTargetTable = meta::NAME_DB_OBJ.get_mut(index.tableName.as_str());
        if dbObjectTargetTable.is_none() {
            throwFormat!("create index failed , target table {} not exist", index.tableName );
        }
        let mut dbObjectTargetTable = dbObjectTargetTable.unwrap();
        // 隐含了index的对象需要是table, 因为是mut的有lock用途, 下边生成index本身的data也不用担心同时表上的数据会有变动
        let targetTable = dbObjectTargetTable.asTableMut()?;

        let tableColumnNames: Vec<&str> = targetTable.columns.iter().map(|tableColumn| tableColumn.name.as_str()).collect();

        for targetColumnName in &index.columnNames {
            if tableColumnNames.contains(&targetColumnName.as_str()) == false {
                throwFormat!("table: {} does not contain column: {}", index.tableName ,targetColumnName);
            }
        }

        // 分配id
        index.id = meta::DB_OBJECT_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        let indexName = index.name.clone();

        // 生成index对应的column family
        self.session.createColFamily(index.name.as_str())?;

        // index对应的垃圾桶的column family,它只是个附庸在index上的纯rocks概念体系里的东西,不是db的概念
        self.session.createColFamily(format!("{}{}", indexName, meta::INDEX_TRASH_SUFFIX).as_str())?;

        // todo 新建index的时候要是表上已经有数据需要当场生成index数据 完成
        self.generateIndexDataForExistingTableData(targetTable, &index)?;

        let indexId = u64ToByteArrRef!(index.id);
        let dbObjectIndex = DBObject::Index(index);

        // 落地 index
        meta::STORE.metaStore.put(indexId, serde_json::to_string(&dbObjectIndex)?.as_bytes())?;
        meta::NAME_DB_OBJ.insert(dbObjectIndex.getName(), dbObjectIndex);

        // todo 如何知道表涉及到的index有哪些,要有table和相应的index的联系 完成
        // 回写更新后的表的信息
        targetTable.indexNames.push(indexName);
        meta::STORE.metaStore.put(u64ToByteArrRef!(targetTable.id), serde_json::to_string(&DBObject::Table(targetTable.clone()))?.as_bytes())?;

        Ok(CommandExecResult::DdlResult)
    }

    /// 当创建index的时候,要是table上已经有数据了需要对这些数据创建索引 <br>
    /// 不使用tx snapshot等概念 直接对数据store本体上手
    fn generateIndexDataForExistingTableData(&self, table: &Table, index: &Index) -> Result<()> {
        let mut dbRawIteratorTable: DBRawIterator = meta::STORE.dataStore.raw_iterator_cf(&Session::getColFamily(index.tableName.as_str())?);
        dbRawIteratorTable.seek(meta::DATA_KEY_PATTERN);

        let indexColumnFamily = Session::getColFamily(index.name.as_str())?;

        let mut indexKeyBuffer = BytesMut::new();

        loop {
            let dataKey = getKeyIfSome!(dbRawIteratorTable);

            if dataKey.starts_with(&[meta::KEY_PREFIX_DATA]) == false {
                break;
            }

            let rowDataBinary = dbRawIteratorTable.value().unwrap();

            let mut scanParams = ScanParams::default();
            scanParams.table = table;
            scanParams.selectedColumnNames = Some(&index.columnNames);

            let rowData = self.readRowDataBinary(rowDataBinary, &scanParams)?.unwrap();

            indexKeyBuffer.clear();

            for indexColumnName in &index.columnNames {
                let columnValue = rowData.get(indexColumnName).unwrap();
                columnValue.encode(&mut indexKeyBuffer)?;
            }

            indexKeyBuffer.put_slice(dataKey);

            meta::STORE.dataStore.put_cf(&indexColumnFamily, indexKeyBuffer.as_ref(), global::EMPTY_BINARY.as_slice())?;

            dbRawIteratorTable.next();
        }

        Ok(())
    }
}