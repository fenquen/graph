use crate::executor::CommandExecutor;
use crate::{key_prefix_add_row_id, meta};
use crate::types::{ColumnFamily, DBRawIterator, TxId};

impl<'session> CommandExecutor<'session> {
    pub fn vaccumData(thresholdTxId: TxId) {
        let tableNames: Vec<String> = meta::TABLE_NAME_TABLE.iter().map(|pair| pair.name.clone()).collect();

        let dataStore = &meta::STORE.dataStore;

        for tableName in tableNames {
            if tableName == meta::COLUMN_FAMILY_NAME_TX_ID {
                continue;
            }

            // dataKey mvccKey pointerKey originDataKeyKey
            // 先去scan mvccKey

            let columnFamily: Option<ColumnFamily> = dataStore.cf_handle(tableName.as_str());
            if let None = columnFamily {
                continue;
            }

            let columnFamily = columnFamily.unwrap();
            let mut dbRawIterator: DBRawIterator = dataStore.raw_iterator_cf(&columnFamily);
            
        }
    }
}