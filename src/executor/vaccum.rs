use crate::executor::CommandExecutor;
use crate::{extractKeyTagFromMvccKey, extractTxIdFromMvccKey, keyPrefixAddRowId, global, meta, u64ToByteArrRef, byte_slice_to_u64, extractRowIdFromKeySlice, extractMvccKeyTagFromPointerKey};
use crate::types::{ColumnFamily, DBRawIterator, TxId};
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use crate::executor::mvcc::BytesMutExt;

impl<'session> CommandExecutor<'session> {
    pub fn vaccumData(thresholdTxIdInclude: TxId) -> Result<()> {
        let tableNames: Vec<String> = meta::TABLE_NAME_TABLE.iter().map(|pair| pair.name.clone()).collect();

        let dataStore = &meta::STORE.dataStore;

        for tableName in tableNames {
            if tableName == meta::COLUMN_FAMILY_NAME_TX_ID {
                continue;
            }

            // dataKey mvccKey pointerKey originDataKeyKey
            let columnFamily: Option<ColumnFamily> = dataStore.cf_handle(tableName.as_str());
            if let None = columnFamily {
                continue;
            }

            let columnFamily = columnFamily.unwrap();
            let mut dbRawIteratorMvccKey: DBRawIterator = dataStore.raw_iterator_cf(&columnFamily);
            let mut dbRawIteratorPointerKey: DBRawIterator = dataStore.raw_iterator_cf(&columnFamily);

            // 先去scan xmax mvccKey
            dbRawIteratorMvccKey.seek(u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PREFIX_MVCC, meta::ROW_ID_INVALID)));

            let mut keyBuffer = BytesMut::with_capacity(meta::POINTER_KEY_BYTE_LEN);

            loop {
                let mvccKey = dbRawIteratorMvccKey.key();
                if let None = mvccKey {
                    break;
                }

                let mvccKey = mvccKey.unwrap();

                // 过头了
                if mvccKey.starts_with(&[meta::KEY_PREFIX_MVCC]) == false {
                    break;
                }

                // keyTag需要是xmax
                if meta::MVCC_KEY_TAG_XMAX != extractKeyTagFromMvccKey!(mvccKey) {
                    continue;
                }

                let rowId = extractRowIdFromKeySlice!(mvccKey);
                let xmax = extractTxIdFromMvccKey!(mvccKey);

                // 说明需要干掉
                if thresholdTxIdInclude >= xmax && xmax != meta::TX_ID_INVALID {
                    for keyPrefix in meta::KEY_PREFIX_DATA..=meta::KEY_PPREFIX_ORIGIN_DATA_KEY {
                        dataStore.delete_range_cf(&columnFamily,
                                                  u64ToByteArrRef!(keyPrefixAddRowId!(keyPrefix, rowId)),
                                                  u64ToByteArrRef!(keyPrefixAddRowId!(keyPrefix, rowId + 1)))?;
                    }

                    continue;
                }

                // thresholdTxIdInclude 后边时候干掉的
                if xmax != meta::TX_ID_INVALID {
                    continue;
                }

                // 以下是原先为了应对txId耗尽回卷应对 其实没有必要
                // 因为不像pg使用的是32bit的,txId使用64bit的tx能够根本上应对该问题
                // pg使用32bit原因还是因为当初考虑的不够了 因为当时硬件性能低下每秒处理上百个tx都是够呛的
                // https://my.oschina.net/postgresqlchina/blog/5547139
                // 64bit的tx 就算每秒100000个tx也能使用600万year不到

                if false {
                    // 这里的都是xmax是0的 说明要保留的 xmin的tx要变为TX_ID_FROZEN
                    // 应对的是data本身部分 也可以说是mvccKey部分
                    keyBuffer.writeDataMvccXminByRowId(rowId, meta::TX_ID_FROZEN);
                    // 如果xmin已经是TX_ID_FROZEN了需要跳过
                    if dataStore.get_cf(&columnFamily, keyBuffer.as_ref())?.is_some() {
                        continue;
                    }
                    dataStore.put_cf(&columnFamily, keyBuffer.as_ref(), global::EMPTY_BINARY)?;

                    // todo 如何应对pointerKey
                    {
                        // 定位到pointerKey部分
                        let pointerKeyPrefix = u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PREFIX_POINTER, rowId));
                        dbRawIteratorPointerKey.seek(pointerKeyPrefix);

                        loop {
                            let pointerKey = dbRawIteratorPointerKey.key();
                            if let None = pointerKey {
                                break;
                            }

                            let pointerKey = pointerKey.unwrap();

                            // 过头了
                            if pointerKey.starts_with(pointerKeyPrefix) == false {
                                break;
                            }

                            // 需要的是xmin
                            if meta::MVCC_KEY_TAG_XMIN != extractMvccKeyTagFromPointerKey!(pointerKey) {
                                continue;
                            }

                            // 需要替换pointerKey末尾的txId
                            keyBuffer.put_slice(&pointerKey[meta::POINTER_KEY_TX_ID_OFFSET..]);
                            keyBuffer.put_u64(meta::TX_ID_FROZEN);

                            dataStore.put_cf(&columnFamily, keyBuffer.as_ref(), global::EMPTY_BINARY)?;
                        }
                    }
                }


                dbRawIteratorMvccKey.next();
            }
        }

        // todo txId应该回到哪里

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::mem;
    use rocksdb::{DB, Options};
    use crate::{byte_slice_to_u64, global, u64ToByteArrRef};
    use crate::types::DBRawIterator;

    #[test]
    pub fn testDeleteInIter() {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, "test").unwrap();

        db.put(u64ToByteArrRef!(1u64), global::EMPTY_BINARY).unwrap();
        db.put(u64ToByteArrRef!(2u64), global::EMPTY_BINARY).unwrap();

        {
            let mut dbRawIterator: DBRawIterator = db.raw_iterator();
            dbRawIterator.seek(u64ToByteArrRef!(1u64));

            if let Some(key) = dbRawIterator.key() {
                db.delete(u64ToByteArrRef!(1u64)).unwrap();
                db.delete(u64ToByteArrRef!(2u64)).unwrap();
            }

            //  println!("{}\n", db.get(u64_to_byte_array_ref!(1u64)).unwrap().is_some());

            //  println!("{}\n", byte_slice_to_u64!(dbRawIterator.key().unwrap()));

            // 需要重弄个iter能看到更改 不然看不到
            let mut dbRawIterator: DBRawIterator = db.raw_iterator();
            dbRawIterator.seek(u64ToByteArrRef!(1u64));
            // dbRawIterator.next();
            println!("{}", dbRawIterator.valid());
            // println!("{}\n", byte_slice_to_u64!(dbRawIterator.key().unwrap()));

            //  dbRawIterator.next();
            //  println!("{}", dbRawIterator.valid());
        }

        mem::drop(db);
        DB::destroy(&options, "test").unwrap();
    }
}