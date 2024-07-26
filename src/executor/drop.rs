use std::mem::{forget, ManuallyDrop};
use std::ptr;
use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use dashmap::mapref::one::RefMut;
use bytes::BufMut;
use crate::{extractDirectionKeyTagFromPointerKey, extractRowIdFromDataKey, extractRowIdFromKeySlice};
use crate::{extractTargetDataKeyFromPointerKey, extractTargetDBObjectIdFromPointerKey, keyPrefixAddRowId, throw, throwFormat};
use crate::meta::{DBObject, Table};
use crate::meta;
use crate::session::Session;

impl<'session> CommandExecutor<'session> {
    pub(super) fn dropTable(&self, tableName: &str) -> Result<CommandExecResult> {
        let mut dbObjectTableRefMut = Session::getDBObjectMutByName(tableName)?;

        let table = match dbObjectTableRefMut.value() {
            DBObject::Table(_) | DBObject::Relation(_) => {
                let columnFamily = Session::getColumnFamily(tableName)?;
                let mut dbRawIterator = self.session.getDBRawIteratorWithoutSnapshot(&columnFamily)?;

                dbRawIterator.seek(&[meta::KEY_PREFIX_POINTER]);

                match dbObjectTableRefMut.value_mut() {
                    DBObject::Table(table) => {
                        // 要是table上有关系关联 不能drop
                        if let Some(pointerKey) = dbRawIterator.key() {
                            if pointerKey.starts_with(&[meta::KEY_PREFIX_POINTER]) {
                                throwFormat!("table can not be dropped ,because it is linked by some relation ");
                            }
                        }

                        table
                    }
                    DBObject::Relation(relation) => {
                        let bufferCapacity = meta::DATA_KEY_BYTE_LEN + meta::KEY_TAG_BYTE_LEN + meta::DB_OBJECT_ID_BYTE_LEN;

                        let mut bufferFrom = self.withCapacityIn(bufferCapacity);
                        let mut bufferTo = self.withCapacityIn(bufferCapacity);

                        // 然而如果是relation的话,那么需要把相应的两边全都清理掉
                        loop {
                            bufferFrom.clear();
                            bufferTo.clear();

                            match dbRawIterator.key() {
                                Some(pointerKey) => {
                                    if pointerKey.starts_with(&[meta::KEY_PREFIX_POINTER]) == false {
                                        break;
                                    }

                                    let directionKeyTag = extractDirectionKeyTagFromPointerKey!(pointerKey);
                                    let targetTableId = extractTargetDBObjectIdFromPointerKey!(pointerKey);

                                    // 需要通过targetTableId得到对应名字,目前效率的话只能去metastore
                                    let targetTableName = match meta::STORE.metaStore.get(targetTableId.to_be_bytes())? {
                                        Some(tableJsonSlice) => {
                                            let table = serde_json::from_slice::<Table>(tableJsonSlice.as_slice())?;
                                            table.name
                                        }
                                        None => panic!("impossible")
                                    };

                                    let targetTableColumnFamily = Session::getColumnFamily(targetTableName.as_str())?;

                                    let targetDataKey = extractTargetDataKeyFromPointerKey!(pointerKey);
                                    let rowId = extractRowIdFromDataKey!(targetDataKey);

                                    bufferFrom.put_u64(keyPrefixAddRowId!(meta::KEY_PREFIX_POINTER, rowId));
                                    bufferTo.put_u64(keyPrefixAddRowId!(meta::KEY_PREFIX_POINTER, rowId));

                                    match directionKeyTag {
                                        meta::POINTER_KEY_TAG_SRC_TABLE_ID => { // relation上游
                                            bufferFrom.put_u8(meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID);
                                            bufferTo.put_u8(meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID);
                                        }
                                        meta::POINTER_KEY_TAG_DEST_TABLE_ID => { // relation下游
                                            bufferFrom.put_u8(meta::POINTER_KEY_TAG_UPSTREAM_REL_ID);
                                            bufferTo.put_u8(meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID);
                                        }
                                        _ => panic!("impossible")
                                    }

                                    bufferFrom.put_u64(relation.id);
                                    bufferTo.put_u64(relation.id + 1);

                                    // 因为其实用不到真正的tx需求,直接使用datastore
                                    self.session.deleteRangeUnderCf(&targetTableColumnFamily, bufferFrom.as_ref(), bufferTo.as_ref())?;
                                }
                                None => break
                            }
                        }

                        relation
                    }
                    _ => panic!("impossible")
                }
            }
            _ => throw!("not table nor relation")
        };

        // 清理相应的index
        for indexName in &table.indexNames {
            self.dropIndex(indexName, true, None)?;
        }

        self.session.dropColFamily(tableName)?;
        self.session.deleteMeta(table.id)?;

        dbObjectTableRefMut.invalidate();

        Ok(CommandExecResult::DdlResult)
    }

    /// drop table, alter table drop columns 都会调用到该函数
    pub(super) fn dropIndex<'a>(&self,
                                indexName: &'a str,
                                droppingTable: bool,
                                dbObjectIndexRefMut: Option<&mut RefMut<'a, String, DBObject>>) -> Result<CommandExecResult> {
        log::info!("drop index: {}", indexName);

        let mut localIndexLock = None;

        // 看似多余其实必要, 不然的话本地引用可能通过其逃逸到外部
        let mut dbObjectIndexRefMut = dbObjectIndexRefMut;

        // 因为dashMap的RefMut不想是java那样是可重入的,要是同1个线程上重复去lock会死锁的
        if dbObjectIndexRefMut.is_none() {
            // 因为生命周期标识是类型系统的1部分,如下的赋值调用需要函数签名用到显式的生命周期标识
            localIndexLock = Some(Session::getDBObjectMutByName(indexName)?);
            dbObjectIndexRefMut = localIndexLock.as_mut();
        }

        let mut dbObjectIndexRefMut = dbObjectIndexRefMut.as_mut().unwrap();

        self.session.dropColFamily(indexName)?;
        // 莫忘了对应的trash
        self.session.dropColFamily(format!("{}{}", indexName, meta::INDEX_TRASH_SUFFIX).as_str())?;
        self.session.deleteMeta(dbObjectIndexRefMut.getId())?;

        // 说明是单独drop这个的index,需要去修改对应的table信息
        if droppingTable == false {
            let index = dbObjectIndexRefMut.asIndex()?;

            let mut dbObjectTableRefMut = Session::getDBObjectMutByName(&index.tableName)?;
            let table = dbObjectTableRefMut.asTableMut()?;

            table.indexNames.retain(|indexNameExist| indexNameExist != indexName);
            self.session.putUpdateMeta(table.id, &DBObject::Table(table.clone()))?;
        }

        dbObjectIndexRefMut.invalidate();

        Ok(CommandExecResult::DdlResult)
    }

    pub(super) fn dropRelation(&self, relationName: &str) -> Result<CommandExecResult> {
        self.dropTable(relationName)
    }
}