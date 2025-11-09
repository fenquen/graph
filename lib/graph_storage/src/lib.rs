#![feature(btree_cursors)]
#![feature(likely_unlikely)]
#![allow(non_snake_case)]
#![feature(ptr_metadata)]
#![feature(rwlock_data_ptr)]
//#![allow(unused)]

extern crate core;

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
mod mem_table_r;
mod lru_cache;
mod bitmap;
mod page_allocator;

#[cfg(test)]
mod tests {
    use crate::db::{DBHeader, DBOption, DB};
    use std::{fs, thread};
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant, SystemTime};

    const ELEM_COUNT: usize = 1024;

    #[test]
    fn testGeneral() {
        let s: [usize; 1] = [0];
        let target = 1;
        // Ok 说明 存在相等的 里边的值v是index
        // Err 说明 不存在, 里边的值v 是大于它的最小的元素的index 特殊情况 数组是空的返回Err(0); 数组元素都要比它小返回Err(数组长度)
        println!("{:?}", s.binary_search_by(|probe| probe.cmp(&target)));
        //assert_eq!(s.binary_search_by(|probe| probe.cmp(&seek)), Err(8));

        println!("{},{}", size_of::<DBHeader>(), align_of::<DBHeader>());


        let a = Arc::new(RwLock::new(1));
        let b = a.clone();

        let readGuardA = a.read().unwrap();
        let readGuardB = b.read().unwrap();
    }

    #[test]
    fn testWrite() {
        _ = fs::remove_dir_all(DBOption::default().dirPath);

        let db = DB::open(None).unwrap();

        let mut tx = db.newTx().unwrap();

        let start = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();

        let mut vec = vec![0; 9];
        let a = Instant::now();
        for a in 0..ELEM_COUNT {
            {
                let v = &mut vec.as_mut_slice()[0..8];
               v.copy_from_slice(&a.to_le_bytes());
            }

            // let aa = a.to_be_bytes();
            tx.set(vec.as_slice(), vec.as_slice()).unwrap();
        }
        println!("set time: {} micro second", a.elapsed().as_micros());

        tx.commit().unwrap();

        let end = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros();
        println!("total time: {} micro second", end - start);

        thread::sleep(Duration::from_secs(10));
        let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        drop(db);
        a.join().unwrap();
    }

    #[test]
    fn testRead() {
        let db = DB::open(None).unwrap();
        let tx = db.newTx().unwrap();

        let mut vec = vec![0; 19];
        let v = vec.as_mut_slice();
        let v = &mut v[0..8];

        for a in 0..ELEM_COUNT {
            v.copy_from_slice(&a.to_le_bytes());
           // let key = a.to_be_bytes();
            assert_eq!(tx.get(v).unwrap().as_ref().unwrap().as_slice(), v);
        }
    }

    #[test]
    fn testContinueWrite() {
        let db = DB::open(None).unwrap();

        let mut tx = db.newTx().unwrap();

        for a in 0..ELEM_COUNT {
            let key = a.to_be_bytes();
            let value = (a * 2).to_be_bytes();
            tx.set(&key, &value).unwrap();
        }

        tx.commit().unwrap();

        thread::sleep(Duration::from_secs(10));
        let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        drop(db);
        a.join().unwrap();
    }

    #[test]
    fn testContinueRead() {
        let db = DB::open(None).unwrap();
        let tx = db.newTx().unwrap();

        for a in 0..ELEM_COUNT {
            let key = a.to_be_bytes();
            let value = (a * 2).to_be_bytes();
            assert_eq!(tx.get(&key).unwrap().as_ref().unwrap().as_slice(), value.as_slice());
        }
    }

    #[test]
    fn testDeleteSome() {
        let db = DB::open(None).unwrap();
        let mut tx = db.newTx().unwrap();

        let key = 700usize.to_be_bytes();
        tx.delete(&key).unwrap();
        tx.commit().unwrap();

        let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        drop(db);
        a.join().unwrap();
    }

    #[test]
    fn testGetSome() {
        let db = DB::open(None).unwrap();

        let tx = db.newTx().unwrap();
        assert_eq!(tx.get(&700usize.to_be_bytes()).unwrap(), None);
        // tx.get(&700usize.to_be_bytes()).unwrap();

        thread::sleep(Duration::from_secs(3600));
        //let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        //drop(db);
        //a.join().unwrap();
    }

    #[test]
    fn testWriteReadDeleteRead() {
        _ = fs::remove_dir_all(DBOption::default().dirPath);

        let db = DB::open(None).unwrap();

        // write
        {
            let mut tx = db.newTx().unwrap();

            for a in 0..ELEM_COUNT {
                let aa = a.to_be_bytes();
                tx.set(&aa, &aa).unwrap();
            }

            tx.commit().unwrap();
        }

        // read
        {
            let tx = db.newTx().unwrap();

            for a in 0..ELEM_COUNT {
                let k = a.to_be_bytes();
                assert_eq!(tx.get(&k).unwrap().as_ref().unwrap().as_slice(), k.as_slice());
            }
        }

        thread::sleep(Duration::from_secs(10));
        let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        drop(db);
        a.join().unwrap();

        return;

        // delete
        {
            let mut tx = db.newTx().unwrap();
            for a in 0..ELEM_COUNT {
                let aa = a.to_be_bytes();
                tx.delete(&aa).unwrap();
            }
            tx.commit().unwrap();
        }

        // read
        {
            let tx = db.newTx().unwrap();

            for a in 0..ELEM_COUNT {
                let k = a.to_be_bytes();
                assert_eq!(tx.get(&k).unwrap(), None);
            }
        }

        let a = unsafe { db.joinHandleMemTableRs.assume_init_read() };
        drop(db);
        a.join().unwrap();
    }
}
