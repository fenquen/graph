use std::sync::atomic::Ordering;
use bytes::{BufMut, BytesMut};
use crate::meta::TableType;
use crate::{global, keyPrefixAddRowId, meta, throw, u64ToByteArrRef};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::parser::command::insert::Insert;
use crate::types::{DataKey, KV, RowId};
use anyhow::Result;
use crate::codec::BinaryCodec;

impl<'session> CommandExecutor<'session> {
    // todo insert时候value的排布要和创建表的时候column的顺序对应 完成
    pub(super) fn insert(&self, insert: &mut Insert) -> Result<CommandExecResult> {
        // 对应的表是不是exist
        let dbObjectTable = self.getDBObjectByName(&insert.tableName)?;
        let table = dbObjectTable.asTable()?;

        let rowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let dataKey: DataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, rowId);

        // 写 data本身的key和value
        let dataKeyBinary = u64ToByteArrRef!(dataKey);
        let (rowDataBinary, rowData) = self.generateInsertValuesBinary(insert, table)?;
        let dataAdd = (dataKeyBinary.to_vec(), rowDataBinary.to_vec()) as KV;

        // 写 xmin xmax 对应的 mvcc key
        let mut mvccKeyBuffer = self.withCapacityIn(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, dataKey)?;

        let origin = self.generateOrigin(dataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&table.name, dataAdd, xminAdd, xmaxAdd, origin);

        // 处理相应的index
        // index的key应该是什么样的 columnData + dataKey
        let mut indexKeyBuffer = self.withCapacityIn(rowDataBinary.len() + meta::DATA_KEY_BYTE_LEN);
        self.generateIndexData(table, &mut indexKeyBuffer, dataKey, &rowData, false)?;

        Ok(CommandExecResult::DmlResult)
    }
}