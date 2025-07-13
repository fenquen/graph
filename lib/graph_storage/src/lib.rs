#![feature(btree_cursors)]
#![feature(likely_unlikely)]
#![allow(non_snake_case)]
#![feature(ptr_metadata)]
#![feature(rwlock_downgrade)]
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
    use crate::db::{DBHeader, DBOption, DB};
    use std::{fs};
    use std::time::SystemTime;

    #[test]
    fn general() {
        let s: [usize; 1] = [0];
        let target = 1;
        // Ok 说明 存在相等的 里边的值v是index
        // Err 说明 不存在, 里边的值v 是大于它的最小的元素的index 特殊情况 数组是空的返回Err(0); 数组元素都要比它小返回Err(数组长度)
        println!("{:?}", s.binary_search_by(|probe| probe.cmp(&target)));
        //assert_eq!(s.binary_search_by(|probe| probe.cmp(&seek)), Err(8));

        println!("{},{}", size_of::<DBHeader>(), align_of::<DBHeader>());
        return;
    }

    #[test]
    fn testWrite() {
        _ = fs::remove_dir_all(DBOption::default().dirPath);

        let db = DB::open(None).unwrap();

        let mut tx = db.newTx().unwrap();

        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();

        for a in 0..1024usize {
            let aa = a.to_be_bytes();
            tx.set(&aa, &aa).unwrap();
        }

        tx.commit().unwrap();

        let end = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        println!("total time: {} micro second", end - start);
    }

    #[test]
    fn testRead() {
        let db = DB::open(None).unwrap();
        let tx = db.newTx().unwrap();

        for a in 0..1024usize {
            let k = a.to_be_bytes();
            assert_eq!(tx.get(&k).unwrap().as_ref().unwrap().as_slice(), k.as_slice());
        }
    }

    #[test]
    fn testWriteRead() {
        _ = fs::remove_dir_all(DBOption::default().dirPath);

        let db = DB::open(None).unwrap();

        {
            let mut tx = db.newTx().unwrap();

            for a in 0..4096usize {
                let aa = a.to_be_bytes();
                tx.set(&aa, &aa).unwrap();
            }

            tx.commit().unwrap();
        }

        {
            let tx = db.newTx().unwrap();

            for a in 0..4096usize {
                let k = a.to_be_bytes();
                assert_eq!(tx.get(&k).unwrap().as_ref().unwrap().as_slice(), k.as_slice());
            }
        }
    }
}
