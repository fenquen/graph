use crate::types::TxId;

pub(crate) const DOT_STR: &str = ".";

pub(crate) const TX_ID_SIZE: usize = size_of::<TxId>();

pub(crate) const MAX_KEY_SIZE: usize = u16::MAX as usize - TX_ID_SIZE;
pub(crate) const MAX_VALUE_SIZE: usize = u32::MAX as usize;