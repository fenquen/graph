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

    #[test]
    fn testWrite() {
        let db = DB::open(None).unwrap();

        let mut tx = db.newTx().unwrap();
        tx.set(&[0], &[1]).unwrap();

        tx.commit().unwrap();
    }

    #[test]
    fn testRead() {
        let db = DB::open(None).unwrap();
        let tx = db.newTx().unwrap();
        assert_eq!(tx.get(&[0]).unwrap(), Some(vec![1]));
    }

    use std::rc::{Rc, Weak};

    struct B {
        // 存储对 A 的弱引用
        a: Weak<A>,
    }

    impl B {
        fn get_a(&self) -> Option<Rc<A>> {
            self.a.upgrade()
        }
    }

    // 定义 struct A
    struct A {
        b: B,
    }

    fn a() {
        let mut a = Rc::new(A {
            b: B {
                // 初始化时，b 中的 a 弱引用为空
                a: Weak::new(),
            },
        });

        let weaka = Rc::downgrade(&a);

        let amut = Rc::get_mut(&mut a).unwrap();
        amut.b.a = weaka;

        if let Some(a) = a.b.get_a() {
            println!("Successfully retrieved A from B");
        } else {
            println!("Failed to retrieve A from B");
        }
    }
}
