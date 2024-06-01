use bytes::BytesMut;
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta;
use crate::parser::Delete;

impl<'session> CommandExecutor<'session> {
    // todo rel不能直接delete 应该先把rel上的点全都取消 rel不存在src和dest的点 然后
    /// 得到满足expr的record 然后把它的xmax变为当前的txId
    pub(super) fn delete(&self, delete: &Delete) -> anyhow::Result<CommandExecResult> {
        let pairs = {
            let table = self.getTableRefByName(delete.tableName.as_str())?;
            self.scanSatisfiedRows(table.value(), delete.filterExpr.as_ref(), None, true, None)?
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