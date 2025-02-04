#![allow(non_snake_case)]
#![allow(unused)]

#[macro_use] // 宏引入到当前mod及其子mod,限当前crate内部使用,需放到打头使用
mod macros;
pub mod db;
mod utils;

#[cfg(test)]
mod tests {
    use crate::db::{DBHeader, DB};
    use crate::utils;

    #[test]
    fn general() {
        DB::open("graph.db").unwrap();
    }
}
