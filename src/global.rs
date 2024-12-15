use std::cell::Cell;
use std::{mem, ptr};
use std::alloc::Global;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use dashmap::DashMap;
use hashbrown::{DefaultHashBuilder, HashMap};
use lazy_static::lazy_static;
use tokio::fs::File;
use tokio::sync::RwLock;
use graph_independent::DummyAllocator;
use hashbrown::raw::RawTable;
use crate::graph_error::GraphError;
use crate::graph_value::GraphValue;
use crate::meta;
use crate::meta::Table;
use crate::types::{Byte, RowData};

thread_local! {
    /// https://www.cnblogs.com/jiangbo4444/p/15932305.html <br>
    /// enum使用json序列化 默认是tagged的 <br>
    /// GraphValue::String("a") 会变为 {"String":"a"} <br>
    /// 有的时候需要的是untagged 序列化为"1"
    /// 有个要点是内部不能使用async函数 不然的话可能会跑到别的os线程上去污染
    pub static UNTAGGED_ENUM_JSON: Cell<bool> = Cell::new(false);
}

pub const TOTAL_DATA_OF_TABLE: u64 = u64::MAX;

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

pub const 回车_CHAR: char = '\r';
pub const 换行_CHAR: char = '\n';

pub const DOT_CHAR: char = '.';
pub const DOT_STR: &str = ".";

pub const EMPTY_BINARY: Vec<Byte> = vec![];

pub const 百分号_CHAR: char = '%';
pub const 百分号_STR: &str = "%";

// todo 20241127 如何对 DUMMY_ROW_DATA 实现 getRowSize() 是 0 需要有个标识来表明它是dummy的
/// 通过强改 hashbrown-0.15.2 和它依赖的 foldhash-0.1.3 源码(private的mod,字段 变为pub的) 实现
pub const DUMMY_ROW_DATA: RowData = const {
    //let hash_builder = unsafe { MaybeUninit::<DefaultHashBuilder>::uninit().assume_init() };
    //let table = unsafe { MaybeUninit::<hashbrown::raw::RawTable<(String, GraphValue)>>::uninit().assume_init() };
    HashMap {
        hash_builder: DefaultHashBuilder {
            per_hasher_seed: 0,
            global_seed: foldhash::seed::global::GlobalSeed {
                _no_accidental_unsafe_init: (),
            },
        },
        table: RawTable {
            table: hashbrown::raw::RawTableInner::new(),
            alloc: Global,
            marker: PhantomData,
        },
        dummy: true,
    }
};