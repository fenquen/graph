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
        if Session::getDBObjectByName(table.name.as_str()).is_ok() {
            if table.createIfNotExist == false {
                throwFormat!("table/relation: {} already exist", table.name);
            }

            return Ok(CommandExecResult::DdlResult);
        }

        table.id = meta::DB_OBJECT_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        // 生成column family
        self.session.createColFamily(table.id)?;

        // todo 使用 u64的tableId 为key 完成
        let tableId = table.id;

        let dbObject =
            if isTable == false {
                DBObject::Relation(table)
            } else {
                DBObject::Table(table)
            };

        self.session.putUpdateMeta(tableId, &dbObject)?;

        // map
        meta::NAME_DB_OBJ.insert(dbObject.getName().to_string(), dbObject);

        Ok(CommandExecResult::DdlResult)
    }

    // 对非unique的index如何应对重复的value,后边添加dataKey
    pub(super) fn createIndex(&self, mut index: Index) -> Result<CommandExecResult> {
        log::info!("create index: {}" , index.name);

        if Session::getDBObjectByName(index.name.as_str()).is_ok() {
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
        index.trashId = meta::DB_OBJECT_ID_COUNTER.fetch_add(1, Ordering::AcqRel);

        let indexName = index.name.clone();

        // 生成index对应的column family
        self.session.createColFamily(index.id)?;

        // index对应的垃圾桶的column family,它只是个附庸在index上的纯rocks概念体系里的东西,不是db的概念
        self.session.createColFamily(index.trashId)?;

        // 新建index的时候要是表上已经有数据需要当场生成index数据
        self.generateIndexDataForExistingTableData(targetTable, &index)?;

        let indexId = index.id;
        let dbObjectIndex = DBObject::Index(index);

        // 落地 index
        self.session.putUpdateMeta(indexId, &dbObjectIndex)?;
        // map 更新
        meta::NAME_DB_OBJ.insert(dbObjectIndex.getName().to_string(), dbObjectIndex);

        // 回写更新后的表的信息落地
        targetTable.indexNames.push(indexName);
        self.session.putUpdateMeta(targetTable.id, &DBObject::Table(targetTable.clone()))?;

        Ok(CommandExecResult::DdlResult)
    }

    // todo 要是create table,insert,create index连在1起的话 该该函数不生效因为读取的是已经提交的 而这时insert的尚未提交
    /// 当创建index的时候,要是table上已经有数据了需要对这些数据创建索引 <br>
    /// 直接对数据store本体上手
    fn generateIndexDataForExistingTableData(&self, table: &Table, index: &Index) -> Result<()> {
        let mut dbRawIteratorTable: DBRawIterator = meta::STORE.dataStore.raw_iterator_cf(&Session::getColumnFamily(table.id)?);
        dbRawIteratorTable.seek(meta::DATA_KEY_PATTERN);

        let indexColumnFamily = Session::getColumnFamily(index.id)?;

        let mut indexKeyBuffer = self.newIn();

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
                columnValue.encode2ByteMut(&mut indexKeyBuffer)?;
            }

            indexKeyBuffer.put_slice(dataKey);

            meta::STORE.dataStore.put_cf(&indexColumnFamily, indexKeyBuffer.as_ref(), global::EMPTY_BINARY.as_slice())?;

            dbRawIteratorTable.next();
        }

        Ok(())
    }
}