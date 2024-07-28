use bytes::BytesMut;
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::{meta, types};
use crate::parser::command::delete::Delete;
use types::CommittedPreProcessor;
use crate::executor::store::{ScanHooks, ScanParams};
use anyhow::Result;
use crate::session::Session;

impl<'session> CommandExecutor<'session> {
    // todo rel不能直接delete 应该先把rel上的点全都取消 rel不存在src和dest的点 然后
    /// 得到满足expr的record 然后把它的xmax变为当前的txId
    pub(super) fn delete(&self, delete: &Delete) -> Result<CommandExecResult> {
        let table = Session::getDBObjectByName(delete.tableName.as_str())?;
        let table = table.asTable()?;

        let targetRowDatas = {
            let scanParams = ScanParams {
                table,
                tableFilter: delete.filterExpr.as_ref(),
                ..Default::default()
            };

            self.scanSatisfiedRows(scanParams, true, ScanHooks::default())?
        };

        let mut buffer = self.withCapacityIn(meta::MVCC_KEY_BYTE_LEN);

        // 遍历添加当前tx对应的xmax
        for (targetDataKey, targetRowData) in targetRowDatas {
            self.generateIndexData(table, &mut buffer, targetDataKey, &targetRowData, true)?;

            let oldXmax = self.generateDeleteDataXmax(&mut buffer, targetDataKey)?;
            self.session.writeDeleteDataMutation(table.id, oldXmax);
        }

        Ok(CommandExecResult::DmlResult)
    }
}