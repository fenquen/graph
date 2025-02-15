#![allow(non_snake_case)]
//#![allow(unused)]

#[macro_use] // 宏引入到当前mod及其子mod,限当前crate内部使用,需放到打头使用
mod macros;
pub mod db;
mod utils;
mod page_header;
mod types;
mod page;
mod tx;
mod constant;

#[cfg(test)]
mod tests {
    use libc::printf;
    use crate::db::{DBOption, DB};

    #[test]
    fn general() {
        let dbOption = DBOption {
            dirPath: "data".to_string(),
            blockSize: 0,
        };

        DB::open(&dbOption).unwrap();
    }
}
