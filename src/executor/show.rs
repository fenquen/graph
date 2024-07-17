use crate::executor::{CommandExecResult, CommandExecutor};
use anyhow::Result;
use serde_json::json;
use crate::meta::DBObject;
use crate::session::Session;
use crate::{meta, throw};

impl<'session> CommandExecutor<'session> {
    pub(super) fn showIndice(&self, dbObject: Option<&DBObject>) -> Result<CommandExecResult> {
        match dbObject {
            Some(dbObject) => {
                let dbObjectName = dbObject.getName();
                let dbObject = Session::getDBObjectByName(dbObjectName.as_str())?;

                let table = match dbObject.value() {
                    DBObject::Table(table) => table,
                    DBObject::Relation(table) => table,
                    _ => throw!("only table and relation have indice")
                };

                Ok(CommandExecResult::SelectResult(table.indexNames.iter().map(|indexName| json!(indexName)).collect()))
            }
            None => {
                let indexNames: Vec<String> = meta::NAME_DB_OBJ.iter().filter_map(|dbObject| dbObject.asIndexOption().map(|index| index.name.clone())).collect();
                Ok(CommandExecResult::SelectResult(indexNames.iter().map(|indexName| json!(indexName)).collect()))
            }
        }
    }

    pub(super) fn showRelations(&self) -> Result<CommandExecResult> {
        let relationNames: Vec<String> = meta::NAME_DB_OBJ.iter().filter_map(|dbObject| dbObject.asRelationOption().map(|relation| relation.name.clone())).collect();
        Ok(CommandExecResult::SelectResult(relationNames.iter().map(|indexName| json!(indexName)).collect()))
    }

    pub(super) fn showTables(&self) -> Result<CommandExecResult> {
        let tableNames: Vec<String> = meta::NAME_DB_OBJ.iter().filter_map(|dbObject| dbObject.asTableOption().map(|table| table.name.clone())).collect();
        Ok(CommandExecResult::SelectResult(tableNames.iter().map(|indexName| json!(indexName)).collect()))
    }
}