use std::fs::File;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;
use lazy_static::lazy_static;
use crate::Table;


pub static mut TABLE_RECORD_FILE: Option<Arc<RwLock<File>>> = None;

lazy_static! {
    pub static ref TABLE_NAME_TABLE: DashMap<String, Table> = DashMap::new();
}

pub const 空格_CHAR: char = ' ';
pub const SPACE_STR: &str = " ";
pub const SPACE_CHAR_BYTE: u8 = b' ';
pub const EMPTY_STR: &str = "";
pub const 逗号_CHAR: char = ',';
pub const 单引号_CHAR: char = '\'';
pub const 括号_CHAR: char = '(';
pub const 括号1_CHAR: char = ')';

pub const 临界_CHARS: &[char] = &[空格_CHAR, 逗号_CHAR, 单引号_CHAR, 括号_CHAR, 括号1_CHAR];
