use crate::types::TxId;

pub(crate) const DOT_STR: &str = ".";

pub(crate) const TX_ID_SIZE: usize = size_of::<TxId>();
pub(crate) const MAX_KEY_SIZE: usize = u16::MAX as usize;