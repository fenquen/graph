#![feature(btree_cursors)]
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
mod cursor;
mod mem_table;

#[cfg(test)]
mod tests {
    use crate::db::DB;
    use std::thread;

    #[test]
    fn general() {
        let s = [0, 1, 1, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55];
        let seek = 7;
        assert_eq!(s.binary_search_by(|probe| probe.cmp(&seek)), Err(8));

        let db = DB::open(None).unwrap();
        let dbClone = db.clone();

        let a = thread::spawn(move || {
            let mut tx = dbClone.newTx().unwrap();
            assert_eq!(tx.get(&[1]).unwrap(), None);

            tx.set(&[0], &[1]).unwrap();

            tx.commit().unwrap();
        });

        let _ = a.join();
    }
}
