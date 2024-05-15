use std::cell::Cell;
use std::mem;
use std::sync::{Arc};
use std::sync::atomic::AtomicU64;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use lazy_static::lazy_static;
use tokio::fs::File;
use tokio::sync::RwLock;
use crate::graph_error::GraphError;
use crate::meta;
use crate::meta::Table;

pub type TxId = u64;

pub type ReachEnd = bool;
pub type DataPosition = meta::RowId;
pub type DataLen = u32;

lazy_static! {
    pub static ref TX_ID_COUNTER: AtomicU64 = AtomicU64::new(TX_ID_MIN);
    pub static ref TABLE_RECORD_FILE: ArcSwap<Option<RwLock<File>>> = ArcSwap::default();
    pub static ref WAL_FILE: ArcSwap<Option<RwLock<File>>> = ArcSwap::default();
}

pub const TX_ID_INVALID: TxId = 0;
pub const TX_ID_MIN: TxId = 3;

/// 类似pg的xmin和xmax用途的长度
pub const TX_ID_LEN: usize = mem::size_of::<TxId>();

pub const WAL_CONTENT_FIELD_LEN: usize = mem::size_of::<DataLen>();
pub const WAL_PREFIX_LEN: usize = TX_ID_LEN + WAL_CONTENT_FIELD_LEN;

/// 如果当前的rowData已是失效的话会指向实际的有效的data的position
pub const ROW_NEXT_POSITION_LEN: usize = mem::size_of::<DataPosition>();
/// 标识rowData长度
pub const ROW_CONTENT_LEN_FIELD_LEN: usize = mem::size_of::<DataLen>();
/// xmin(u64) + xmax(u64) + next position(u64) + content len(u32)
pub const ROW_PREFIX_LEN: usize = TX_ID_LEN + TX_ID_LEN + ROW_NEXT_POSITION_LEN + ROW_CONTENT_LEN_FIELD_LEN;

thread_local! {
    /// https://www.cnblogs.com/jiangbo4444/p/15932305.html <br>
    /// enum使用json序列化 默认是tagged的 <br>
    /// GraphValue::String("a") 会变为 {"String":"a"} <br>
    /// 有的时候需要的是untagged 序列化为"1"
    /// 有个要点是内部不能使用async函数 不然的话可能会跑到别的os线程上去污染
    pub static UNTAGGED_ENUM_JSON: Cell<bool> = Cell::new(false);
}

pub const TOTAL_DATA_OF_TABLE: u64 = u64::MAX;

pub type Byte = u8;

// --------------------------------------------------------

pub const SPACE_CHAR: char = ' ';
pub const SPACE_STR: &str = " ";

pub const SPACE_CHAR_BYTE: u8 = b' ';

pub const EMPTY_STR: &str = "";

pub const 逗号_CHAR: char = ',';
pub const 逗号_STR: &str = ",";

pub const 单引号_CHAR: char = '\'';

pub const 圆括号_CHAR: char = '(';
pub const 圆括号_STR: &str = "(";

pub const 圆括号1_CHAR: char = ')';
pub const 圆括号1_STR: &str = ")";

pub const 分号_CHAR: char = ';';
pub const 冒号_CHAR: char = ':';

pub const 等号_CHAR: char = '=';
pub const 等号_STR: &str = "=";

pub const 小于_CHAR: char = '<';
pub const 小于_STR: &str = "<";

pub const 大于_CHAR: char = '>';
pub const 大于_STR: &str = ">";

pub const 感叹_CHAR: char = '!';
pub const 感叹_STR: &str = "!";

pub const 不等_STR: &str = "!=";

pub const 小于等于_STR: &str = "<=";

pub const 大于等于_STR: &str = ">=";

pub const 加号_CHAR: char = '+';
pub const 除号_CHAR: char = '/';
pub const 乘号_CHAR: char = '*';
pub const 减号_CHAR: char = '-';

pub const 方括号_CHAR: char = '[';
pub const 方括号_STR: &str = "[";
pub const 方括号1_CHAR: char = ']';
pub const 方括号1_STR: &str = "]";
