#![feature(btree_cursors)]
#![feature(likely_unlikely)]
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
mod page_elem;

#[cfg(test)]
mod tests {
    use crate::db::{DBOption, DB};
    use std::{fs, thread};
    use std::time::SystemTime;

    #[test]
    fn general() {
        let s: [usize; 0] = [];
        let seek = 1;
        println!(" {:?}", s.binary_search_by(|probe| probe.cmp(&seek)));
        //assert_eq!(s.binary_search_by(|probe| probe.cmp(&seek)), Err(8));

        return;

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

    #[test]
    fn testWrite() {
        _ = fs::remove_dir_all(DBOption::default().dirPath);

        let db = DB::open(None).unwrap();

        let mut tx = db.newTx().unwrap();

        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();

        for a in 0..1024usize {
            let aa = a.to_be_bytes();
            tx.set(&aa, &aa).unwrap();
        }

        tx.commit().unwrap();

        let end = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        println!("total time: {} ms", end - start);
    }

    #[test]
    fn testRead() {
        let db = DB::open(None).unwrap();
        let tx = db.newTx().unwrap();

        for a in 0..1024usize {
            let aa = a.to_be_bytes();
            assert_eq!(tx.get(&aa).unwrap(), Some(aa.to_vec()));
        }
    }
}
