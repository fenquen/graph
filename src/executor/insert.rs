use std::sync::atomic::Ordering;
use bytes::BytesMut;
use crate::meta::TableType;
use crate::parser::Insert;
use crate::{key_prefix_add_row_id, meta, throw, u64_to_byte_array_reference};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::types::{DataKey, KV, RowId};

impl<'session> CommandExecutor<'session> {
    // todo insert时候value的排布要和创建表的时候column的顺序对应 完成
    pub fn insert(&self, insert: &mut Insert) -> anyhow::Result<CommandExecResult> {
        // 对应的表是不是exist
        let table = self.getTableRefByName(&insert.tableName)?;

        // 不能对relation使用insert into
        if let TableType::Relation = table.type0 {
            throw!(&format!("{} is a RELATION , can not use insert into on RELATION", insert.tableName));
        }

        let rowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let dataKey: DataKey = key_prefix_add_row_id!(meta::KEY_PREFIX_DATA, rowId);

        // 写 data本身的key和value
        let dataAdd = {
            let dataKeyBinary = u64_to_byte_array_reference!(dataKey);
            let rowDataBinary = self.generateInsertValuesBinary(insert, &*table)?;

            (dataKeyBinary.to_vec(), rowDataBinary.to_vec()) as KV
        };

        // 写 xmin xmax 对应的 mvcc key
        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, dataKey)?;

        let origin = self.generateOrigin(dataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&table.name, dataAdd, xminAdd, xmaxAdd, origin);

        Ok(CommandExecResult::DmlResult)
    }
}