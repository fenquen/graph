use crate::executor::CommandExecutor;
use crate::{extractKeyTagFromMvccKey, extractTxIdFromMvccKey, keyPrefixAddRowId, meta, u64ToByteArrRef, byte_slice_to_u64, extractRowIdFromKeySlice};
use crate::types::{ColumnFamily, DBRawIterator, TxId};
use anyhow::Result;

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
            let mut dbRawIterator: DBRawIterator = dataStore.raw_iterator_cf(&columnFamily);

            // 先去scan xmax mvccKey
            dbRawIterator.seek(u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PREFIX_MVCC, meta::ROW_ID_INVALID)));

            loop {
                let mvccKey = dbRawIterator.key();
                if let None = mvccKey {
                    break;
                }

                let mvccKey = mvccKey.unwrap();

                // 过头了
                if mvccKey.starts_with(&[meta::KEY_PREFIX_MVCC]) == false {
                    break;
                }

                // keyTag需要是xmax
                if extractKeyTagFromMvccKey!(mvccKey) == meta::MVCC_KEY_TAG_XMAX {
                    // 说明需要干掉
                    if thresholdTxIdInclude >= extractTxIdFromMvccKey!(mvccKey) {
                        let rowId = extractRowIdFromKeySlice!(mvccKey);

                        for keyPrefix in meta::KEY_PREFIX_DATA..=meta::KEY_PPREFIX_ORIGIN_DATA_KEY {
                            dataStore.delete_range_cf(&columnFamily,
                                                      u64ToByteArrRef!(keyPrefixAddRowId!(keyPrefix, rowId)),
                                                      u64ToByteArrRef!(keyPrefixAddRowId!(keyPrefix, rowId + 1)))?;
                        }
                    }
                }

                dbRawIterator.next();
            }
        }

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