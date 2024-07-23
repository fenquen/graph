use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use crate::meta::{Column, DBObject};
use crate::parser::command::alter::{Alter, AlterTable};
use crate::session::Session;
use crate::{throw, throwFormat, utils};

impl<'session> CommandExecutor<'session> {
    pub(super) fn alter(&self, alter: &Alter) -> Result<CommandExecResult> {
        match alter {
            Alter::AlterTable(alterTable) => {
                match alterTable {
                    AlterTable::DropColumns { tableName, columnNames } => {}
                    AlterTable::AddColumns { tableName, columns } => {}
                    AlterTable::Rename(s) => {}
                }
            }
            Alter::AlterIndex { .. } => {}
            Alter::AlterRelation { .. } => {}
        }

        Ok(CommandExecResult::DdlResult)
    }

    fn dropColumns(&self, tableName: &str, columnNames: &[String]) -> Result<()> {
        Ok(())
    }

    fn addColumns(&self, tableName: &str, columns: &[Column]) -> Result<()> {
        let mut dbObject = Session::getDBObjectMutByName(tableName)?;

        // 提取table对象
        let table = match dbObject.value_mut() {
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



        Ok(())
    }
}