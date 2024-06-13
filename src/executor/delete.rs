use bytes::BytesMut;
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::{meta, types};
use crate::parser::command::delete::Delete;
use types::ScanCommittedPreProcessor;
use crate::executor::store::{ScanHooks, ScanParams};

impl<'session> CommandExecutor<'session> {
    // todo rel不能直接delete 应该先把rel上的点全都取消 rel不存在src和dest的点 然后
    /// 得到满足expr的record 然后把它的xmax变为当前的txId
    pub(super) fn delete(&self, delete: &Delete) -> anyhow::Result<CommandExecResult> {
        let pairs = {
            let table = self.getTableRefByName(delete.tableName.as_str())?;

            let scanParams = ScanParams {
                table: table.value(),
                tableFilter: delete.filterExpr.as_ref(),
                ..Default::default()
            };

            self.scanSatisfiedRows(scanParams, true, ScanHooks::default())?
        };

        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);

        // 遍历添加当前tx对应的xmax
        for (dataKey, _) in pairs {
            let oldXmax = self.generateDeleteDataXmax(&mut mvccKeyBuffer, dataKey)?;
            self.session.writeDeleteDataMutation(&delete.tableName, oldXmax);
        }

        Ok(CommandExecResult::DmlResult)
    }
}