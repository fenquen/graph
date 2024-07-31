use std::path::Path;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use std::sync::Arc;
use crate::impls::state_machine::RaftStateMachineImpl;
use crate::impls::storage::RaftLogReaderStorageImpl;

pub mod storage;
pub mod state_machine;
pub mod network;

pub(crate) async fn newStorageAndStateMachine(dirPath: impl AsRef<Path>) -> (RaftLogReaderStorageImpl, RaftStateMachineImpl) {
    let mut options = Options::default();
    options.create_missing_column_families(true);
    options.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let db = Arc::new(DB::open_cf_descriptors(&options, dirPath, vec![store, logs]).unwrap());

    let raftStorage = RaftLogReaderStorageImpl::new(db.clone());
    let raftStateMachine = RaftStateMachineImpl::new(db).await.unwrap();

    (raftStorage, raftStateMachine)
}