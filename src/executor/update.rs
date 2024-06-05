use std::collections::HashMap;
use std::sync::atomic::Ordering;
use bytes::BytesMut;
use rocksdb::{Direction, IteratorMode};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::meta::TableType;
use crate::{extractRowIdFromDataKey, extractRowIdFromKeySlice,
            keyPrefixAddRowId, meta, throw, u64ToByteArrRef, byte_slice_to_u64};
use crate::codec::BinaryCodec;
use crate::executor::store::ScanHooks;
use crate::expr::Expr;
use crate::graph_error::GraphError;
use crate::graph_value::GraphValue;
use crate::parser::command::update::Update;
use crate::types::{Byte, ColumnFamily, DataKey, DBIterator, KV, RowData, RowId, TableMutations};
use crate::types::{ScanCommittedPreProcessor, ScanCommittedPostProcessor, ScanUncommittedPreProcessor, ScanUncommittedPostProcessor};
use anyhow::Result;

impl<'session> CommandExecutor<'session> {
    // todo 要是point还有rel的联系不能update 完成
    pub(super) fn update(&self, update: &Update) -> Result<CommandExecResult> {
        let table = self.getTableRefByName(update.tableName.as_str())?;

        if let TableType::Relation = table.type0 {
            throw!("can not use update on relation");
        }

        let columnName_column = {
            let mut columnName_column = HashMap::with_capacity(table.columns.len());
            for column in &table.columns {
                columnName_column.insert(column.name.to_string(), column.clone());
            }

            columnName_column
        };

        // 要是data有link的话 通过抛异常来跳出scanSatisfiedRows的循环
        let testCommittedDataHasBeenLinked =
            |columnFamily: &ColumnFamily, dataKey: DataKey, rowData: &RowData| {
                let rowId = extractRowIdFromDataKey!(dataKey);

                // todo pointerKey应该同时到committed和uncommitted去搜索
                let pointerKeyPrefix = u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PREFIX_POINTER, rowId));

                let mut dbIterator: DBIterator = self.session.getSnapshot()?.iterator_cf(columnFamily, IteratorMode::From(pointerKeyPrefix, Direction::Forward));
                if let Some(kv) = dbIterator.next() {
                    let (pointerKey, _) = kv?;

                    // 说明有该data条目对应的pointerKey 报错
                    if pointerKey.starts_with(pointerKeyPrefix) {
                        throw!("update can not execute, because data has been linked");
                    }
                }

                Result::<bool>::Ok(true)
            };

        let testUncommittedDataHasBeenLinked =
            |tableMutations: &TableMutations, dataKey: DataKey, rowData: &RowData| {
                Result::<bool>::Ok(true)
            };

        // 这里要使用post体系 基于满足普通update的前提
        let scanHooks = ScanHooks {
            scanCommittedPreProcessor: Option::<Box<dyn ScanCommittedPreProcessor>>::None,
            scanCommittedPostProcessor: Some(testCommittedDataHasBeenLinked),
            scanUncommittedPreProcessor: Option::<Box<dyn ScanUncommittedPreProcessor>>::None,
            scanUncommittedPostProcessor: Some(testUncommittedDataHasBeenLinked),
        };

        let mut pairs =
            self.scanSatisfiedRows(table.value(),
                                   update.filterExpr.as_ref(),
                                   None,
                                   true,
                                   scanHooks)?;

        enum A<'a> {
            DirectValue(GraphValue),
            NeedCalc(&'a Expr),
        }

        let mut columnName_a: HashMap<String, A> = HashMap::with_capacity(update.columnName_expr.len());

        let compatibleCheck = |columnName: &String, columnValue: &GraphValue| {
            match columnName_column.get(columnName) {
                Some(column) => {
                    if column.type0.compatible(columnValue) == false {
                        throw!(&format!("table:{} , column:{}, is not compatilbe with value:{:?}", update.tableName, columnName, columnValue));
                    }
                }
                None => throw!(&format!("table:{} has no column named:{}", update.tableName, columnName)),
            }

            Result::<(), GraphError>::Ok(())
        };

        // todo logical优化
        // column expr能直接计算的先计算 不要到后边的遍历里边重复计算了
        for (columnName, columnExpr) in &update.columnName_expr {
            if columnExpr.needAcutalRowData() {
                columnName_a.insert(columnName.to_string(), A::NeedCalc(columnExpr));
            } else {
                let columnValue = columnExpr.calc(None)?;

                // update设置的值要和column type 相同
                compatibleCheck(columnName, &columnValue)?;

                columnName_a.insert(columnName.to_string(), A::DirectValue(columnValue));
            }
        }

        // todo update的时候能不能直接从binary维度上更改row
        for (dataKey, rowData) in &mut pairs {
            for (columnName, a) in &columnName_a {
                match a {
                    A::NeedCalc(expr) => {
                        let columnValue = expr.calc(Some(rowData))?;

                        // update设置的值要和column type 相同
                        compatibleCheck(columnName, &columnValue)?;

                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                    A::DirectValue(columnValue) => {
                        rowData.insert(columnName.to_string(), columnValue.clone());
                    }
                }
            }

            // todo 各个column的value都在1道使得update时候只能整体来弄太耦合了 后续设想能不能各个column保存到单独的key

            let value = {
                let mut destByteSlice = BytesMut::new();
                // 需要以表定义里边的column顺序来序列化
                for column in &table.columns {
                    let columnValue = rowData.get(&column.name).unwrap();
                    columnValue.encode(&mut destByteSlice)?;
                }

                destByteSlice.freeze()
            };
            let value = value.as_ref();

            // 写老的data的xmax
            let mut mvccKeyBuffer = BytesMut::with_capacity(meta::MVCC_KEY_BYTE_LEN);
            let oldXmax = self.generateDeleteDataXmax(&mut mvccKeyBuffer, *dataKey)?;

            // 写新的data
            let newRowId: RowId = table.rowIdCounter.fetch_add(1, Ordering::AcqRel);
            let newDataKey = keyPrefixAddRowId!(meta::KEY_PREFIX_MVCC,newRowId);
            let newData: KV = (u64ToByteArrRef!(newDataKey).to_vec(), value.to_vec());

            // 写新的data的xmin xmax
            let (newXmin, newXmax) = self.generateAddDataXminXmax(&mut mvccKeyBuffer, newDataKey)?;

            let origin = self.generateOrigin(newDataKey, *dataKey);

            self.session.writeUpdateDataMutation(&table.name, oldXmax, newData, newXmin, newXmax, origin);
        }

        Ok(CommandExecResult::DmlResult)
    }
}