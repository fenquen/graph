use std::sync::{Arc};
use dashmap::DashMap;
use lazy_static::lazy_static;
use tokio::fs::File;
use tokio::sync::RwLock;
use crate::graph_error::GraphError;
use crate::meta::Table;

pub static mut TABLE_RECORD_FILE: Option<Arc<RwLock<File>>> = None;

lazy_static! {
    pub static ref TABLE_NAME_TABLE: DashMap<String, Table> = DashMap::new();
}

pub const 空格_CHAR: char = ' ';
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
pub const 方括号1_STR: &str = "[";

pub const ROW_DATA_LEN_FIELD_LEN: usize = 4;

