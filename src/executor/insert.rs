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
        let dbObject = self.getDBObjectByName(&insert.tableName)?;
        let table = dbObject.asTable()?;

        let rowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
        let dataKey: DataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_DATA, rowId);

        // 写 data本身的key和value
        let dataKeyBinary = u64ToByteArrRef!(dataKey);
        let (rowDataBinary, rowData) = self.generateInsertValuesBinary(insert, &*table)?;
        let dataAdd = (dataKeyBinary.to_vec(), rowDataBinary.to_vec()) as KV;

        // 写 xmin xmax 对应的 mvcc key
        let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
        let (xminAdd, xmaxAdd) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, dataKey)?;

        let origin = self.generateOrigin(dataKey, meta::DATA_KEY_INVALID);

        self.session.writeAddDataMutation(&table.name, dataAdd, xminAdd, xmaxAdd, origin);

        // index的key应该是什么样的 columnData + dataKey
        {
            let mut indexKeyBuffer = BytesMut::with_capacity(rowDataBinary.len() + meta::DATA_KEY_BYTE_LEN);

            // 遍历各个index
            for indexName in &table.indexNames {
                let dbObject = self.getDBObjectByName(indexName)?;
                let index = dbObject.asIndex()?;

                assert_eq!(table.name, index.tableName);

                indexKeyBuffer.clear();

                // 遍历了index的各个column
                for targetColumnName in &index.columnNames {
                    let columnValue = rowData.get(targetColumnName).unwrap();
                    columnValue.encode(&mut indexKeyBuffer)?;
                }

                indexKeyBuffer.put_slice(dataKeyBinary);

                self.session.writeAddIndexMutation(&index.name, (indexKeyBuffer.to_vec(), global::EMPTY_BINARY));
            }
        }

        Ok(CommandExecResult::DmlResult)
    }
}