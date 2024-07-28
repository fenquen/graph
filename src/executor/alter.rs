use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use bytes::BufMut;
use rocksdb::DB;
use crate::meta::{Column, DBObject, DBObjectTrait};
use crate::parser::command::alter::{Alter, AlterTable};
use crate::session::Session;
use crate::{meta, throw, throwFormat, utils};
use crate::codec::{BinaryCodec, SliceWrapper};
use crate::graph_value::GraphValue;
use crate::types::SessionVec;

impl<'session> CommandExecutor<'session> {
    pub(super) fn alter(&self, alter: &Alter) -> Result<CommandExecResult> {
        match alter {
            Alter::AlterIndex { .. } => {}
            Alter::AlterTable(alterTable) => {
                match alterTable {
                    AlterTable::DropColumns {
                        tableName,
                        cascade,
                        columnNames2Drop
                    } => self.alterTableDropColumns(tableName, *cascade, columnNames2Drop)?,
                    AlterTable::AddColumns {
                        tableName,
                        columns2Add
                    } => self.alterTableAddColumns(tableName, columns2Add)?,
                    AlterTable::Rename { oldName, newName } => self.alterTableRename(oldName, newName)?
                }
            }
            Alter::AlterRelation { .. } => {}
        }

        Ok(CommandExecResult::DdlResult)
    }

    fn alterTableDropColumns(&self, tableName: &str, cascade: bool, columnNames2Drop: &[String]) -> Result<()> {
        let mut dbObjectTableRefMut = Session::getDBObjectMutByName(tableName)?;

        // 提取table对象
        let table = match dbObjectTableRefMut.value_mut() {
            DBObject::Table(table) => table,
            DBObject::Relation(table) => table,
            _ => throwFormat!("{tableName} is neither table nor relation")
        };

        let existColumnNames: Vec<&String> = table.columns.iter().map(|column| &column.name).collect();

        // 不能把全部的column都dtop掉
        if existColumnNames.len() == columnNames2Drop.len() {
            throwFormat!("can not drop all coulumns of table:{tableName}");
        }

        // 要drop的column的名字在表里没有
        for columnName in columnNames2Drop {
            if existColumnNames.contains(&columnName) == false {
                throwFormat!("{columnName} does not exist in {tableName}");
            }
        }

        // 如果drop掉的column涉及到索引如何应对 如果未写cascade那么报错失败 要写的话级联干掉
        for indexName in &table.indexNames.clone() {
            let mut dbObjectIndexRefMut = Session::getDBObjectMutByName(indexName)?;
            let index = dbObjectIndexRefMut.asIndexMut()?;

            // 涉及到了现有的index, 要是cascade不存在的话报错失败
            let intersect = utils::intersection(&index.columnNames, columnNames2Drop);
            if intersect.is_empty() == false {
                if cascade == false {
                    throwFormat!(" table:{tableName}, index:{indexName}, columns:{intersect:?} will be dropped, try to use cascade");
                }

                // 级联drop掉涉及到的index
                self.dropIndex(indexName, Some(table), Some(index))?;
            }
        }

        // 干掉column对应的数据部分的
        {
            let columnFamily = Session::getColumnFamily(table.id)?;
            let mut dbRawIterator = self.session.getDBRawIteratorWithoutSnapshot(&columnFamily)?;

            dbRawIterator.seek_to_first();

            let mut valueBuffer = self.newIn();

            loop {
                match (dbRawIterator.key(), dbRawIterator.value()) {
                    (Some(key), Some(value)) => {
                        // 确保只修改dataKey部分
                        if key.starts_with(&[meta::KEY_PREFIX_DATA]) == false {
                            break;
                        }

                        let mut sliceWrapper = SliceWrapper::new(value);
                        let columnValues = SessionVec::<GraphValue>::decodeFromSliceWrapper(&mut sliceWrapper, Some(self))?;

                        assert_eq!(table.columns.len(), columnValues.len());

                        let mut columnValuesNew = self.vecWithCapacityIn(columnValues.len());

                        for (column, columnValue) in table.columns.iter().zip(columnValues.into_iter()) {
                            if columnNames2Drop.contains(&column.name) {
                                continue;
                            }

                            columnValuesNew.push(columnValue);
                        }

                        valueBuffer.clear();
                        columnValuesNew.encode2ByteMut(&mut valueBuffer)?;

                        meta::STORE.dataStore.put_cf(&columnFamily, key, valueBuffer.as_ref())?;
                    }
                    (None, Some(_)) | (Some(_), None) => panic!("impossible"),
                    (None, None) => break,
                }

                dbRawIterator.next();
            }
        }

        //  变更对应的table的meta数据
        table.columns.retain(|column| columnNames2Drop.contains(&column.name) == false);
        self.session.putUpdateMeta(table.id, &DBObject::Table(table.clone()))?;

        Ok(())
    }

    fn alterTableAddColumns(&self, tableName: &str, columns2Add: &[Column]) -> Result<()> {
        let mut dbObjectTableRefMut = Session::getDBObjectMutByName(tableName)?;

        // 提取table对象
        let table = match dbObjectTableRefMut.value_mut() {
            DBObject::Table(table) => table,
            DBObject::Relation(table) => table,
            _ => throwFormat!("{tableName} is neither table nor relation")
        };

        // 新增column的名字不能有重复
        let existColumnNames: Vec<&String> = table.columns.iter().map(|column| &column.name).collect();
        let addColumnNames: Vec<&String> = columns2Add.iter().map(|column| &column.name).collect();
        let intersect = utils::intersection(existColumnNames.as_slice(), addColumnNames.as_slice());
        if intersect.is_empty() == false {
            throwFormat!("column {intersect:?} has already exist");
        }

        let bufferNewAddColumnValues = {
            let mut newAddColumnValues = self.vecWithCapacityIn(columns2Add.len());

            for column in columns2Add {
                let defaultValue = match (column.nullable, &column.defaultValue) {
                    (_, Some(default)) => GraphValue::try_from(default)?,
                    (false, None) => throwFormat!("column: {} is not nullable and there is no default value", column.name),
                    (true, None) => GraphValue::Null,
                };

                newAddColumnValues.push(defaultValue);
            }

            let mut bufferNewAddColumnValues = self.newIn();
            newAddColumnValues.encode2ByteMut(&mut bufferNewAddColumnValues)?;

            bufferNewAddColumnValues
        };

        let columnFamily = Session::getColumnFamily(table.id)?;
        let mut dbRawIterator = self.session.getDBRawIteratorWithoutSnapshot(&columnFamily)?;

        dbRawIterator.seek_to_first();

        let mut valueBuffer = self.newIn();

        loop {
            match (dbRawIterator.key(), dbRawIterator.value()) {
                (Some(key), Some(value)) => {
                    // 确保只修改dataKey部分
                    if key.starts_with(&[meta::KEY_PREFIX_DATA]) == false {
                        break;
                    }

                    valueBuffer.clear();

                    valueBuffer.put_slice(value);
                    valueBuffer.put_slice(bufferNewAddColumnValues.as_ref());

                    meta::STORE.dataStore.put_cf(&columnFamily, key, valueBuffer.as_ref())?;
                }
                (None, Some(_)) | (Some(_), None) => panic!("impossible"),
                (None, None) => break,
            }

            dbRawIterator.next();
        }

        //  变更对应的table的meta数据
        for column in columns2Add {
            table.columns.push(column.clone());
        }
        self.session.putUpdateMeta(table.id, &DBObject::Table(table.clone()))?;

        Ok(())
    }

    /// alter table a rename to b
    fn alterTableRename(&self, oldName: &str, newName: &str) -> Result<()> {
        let mut dbObjectTableRefMut = Session::getDBObjectMutByName(oldName)?;

        let table = dbObjectTableRefMut.asTableMut()?;

        // table上的各index也要相应的更改
        for indexName in &table.indexNames {
            let mut dbObjectIndexRefMut = Session::getDBObjectMutByName(indexName)?;

            let index = dbObjectIndexRefMut.asIndexMut()?;
            index.tableName = newName.to_string();

            self.session.putUpdateMeta(index.id, &DBObject::Index(index.clone()))?;
        }

        let mut newTable = table.clone();
        newTable.name = newName.to_string();
        self.session.putUpdateMeta(table.id, &DBObject::Table(newTable.clone()))?;
        meta::NAME_DB_OBJ.insert(newName.to_string(), DBObject::Table(newTable));

        table.invalidate();

        Ok(())
    }
}