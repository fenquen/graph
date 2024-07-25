use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use bytes::BufMut;
use crate::meta::{Column, DBObject};
use crate::parser::command::alter::{Alter, AlterTable};
use crate::session::Session;
use crate::{meta, throw, throwFormat, utils};
use crate::codec::{BinaryCodec, SliceWrapper};
use crate::graph_value::GraphValue;
use crate::types::SessionVec;

impl<'session> CommandExecutor<'session> {
    pub(super) fn alter(&self, alter: &Alter) -> Result<CommandExecResult> {
        match alter {
            Alter::AlterTable(alterTable) => {
                match alterTable {
                    AlterTable::DropColumns { tableName, cascade, columnNames } => {}
                    AlterTable::AddColumns { tableName, columns } => self.addColumns(tableName, columns)?,
                    AlterTable::Rename(s) => {}
                }
            }
            Alter::AlterIndex { .. } => {}
            Alter::AlterRelation { .. } => {}
        }

        Ok(CommandExecResult::DdlResult)
    }

    fn dropColumns(&self, tableName: &str, cascade: bool, columnNames2Drop: &[String]) -> Result<()> {
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

        // todo 如果drop掉的column涉及到索引如何应对 如果未写cascade那么报错失败 要写的话级联干掉
        for indexName in &table.indexNames {
            let dbObjectIndexRef = Session::getDBObjectByName(indexName)?;
            let index = dbObjectIndexRef.asIndex()?;

            // 涉及到了现有的index, 要是cascade不存在的话报错失败
            let intersect = utils::intersect(&index.columnNames, columnNames2Drop);
            if intersect.is_empty() == false {
                throwFormat!("table:{tableName}, index:{indexName}, columns:{intersect} will be dropped");
            }
        }

        let columnFamily = Session::getColumnFamily(tableName)?;
        let mut dbRawIterator = self.session.getDBRawIterator(&columnFamily)?;

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

        Ok(())
    }

    fn addColumns(&self, tableName: &str, columns: &[Column]) -> Result<()> {
        let mut dbObjectTableRefMut = Session::getDBObjectMutByName(tableName)?;

        // 提取table对象
        let table = match dbObjectTableRefMut.value_mut() {
            DBObject::Table(table) => table,
            DBObject::Relation(table) => table,
            _ => throwFormat!("{tableName} is neither table nor relation")
        };

        // 新增column的名字不能有重复
        let existColumnNames: Vec<&String> = table.columns.iter().map(|column| &column.name).collect();
        let addColumnNames: Vec<&String> = columns.iter().map(|column| &column.name).collect();
        let intersect = utils::intersect(existColumnNames.as_slice(), addColumnNames.as_slice());
        if intersect.is_empty() == false {
            throwFormat!("column {intersect:?} has already exist");
        }

        let bufferNewAddColumnValues = {
            let mut newAddColumnValues = self.vecWithCapacityIn(columns.len());

            for column in columns {
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

        let columnFamily = Session::getColumnFamily(tableName)?;
        let mut dbRawIterator = self.session.getDBRawIterator(&columnFamily)?;

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

        Ok(())
    }
}