use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::{alloc, ptr};
use std::alloc::Layout;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use anyhow::format_err;

/// 越过了rust的兜底 以不可变引用对外提供像原来的c/c++ java那样 <br>
/// 使用它的时候需要知道风险
pub struct TrickyContainer<T> {
    data: *mut T,
    used: *mut bool,
}

impl<T> TrickyContainer<T> {
    pub fn new() -> TrickyContainer<T> {
        TrickyContainer {
            data: unsafe { alloc::alloc_zeroed(Layout::new::<T>()) as *mut T },
            used: unsafe { alloc::alloc_zeroed(Layout::new::<bool>()) as *mut bool },
        }
    }

    pub fn set(&self, t: T) {
        unsafe {
            if *self.used {
                let _old = ptr::replace(self.data, t);
            } else {
                ptr::write(self.data, t);
                ptr::write::<bool>(self.used, true);
            }
        }
    }
}

impl<T> Drop for TrickyContainer<T> {
    fn drop(&mut self) {
        unsafe {
            if self.data as usize != 0 {
                ptr::drop_in_place(self.data);
                alloc::dealloc(self.data as *mut u8, Layout::new::<T>());
            }
        }
    }
}

impl<T> TrickyContainer<T> {
    #[inline]
    pub fn getRef(&self) -> &T {
        unsafe { &*self.data }
    }

    #[inline]
    pub fn getRefMut(&self) -> &mut T {
        unsafe { &mut *self.data }
    }

    #[inline]
    pub fn getAddr(&self) -> usize {
        self.data as *const u8 as usize
    }

    #[inline]
    pub fn equals(&self, other: &TrickyContainer<T>) -> bool {
        self.getAddr() == other.getAddr()
    }
}

/// 交集
pub fn intersect<T: Clone + PartialEq>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().filter(|&t| b.contains(t)).map(|destDataKey| destDataKey.clone()).collect()
}

impl<T> Deref for TrickyContainer<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl<T> DerefMut for TrickyContainer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.data }
    }
}

unsafe impl<T> Send for TrickyContainer<T> {}

unsafe impl<T> Sync for TrickyContainer<T> {}

pub trait HashMapExt<K, V, S = RandomState> {
    fn getMutWithDefault<Q: ?Sized>(&mut self, k: &Q) -> &mut V
        where
            K: Borrow<Q> + From<Q>,
            Q: Hash + Eq + Clone,
            V: Default;
}

impl<K: Eq + Hash, V, S: BuildHasher> HashMapExt<K, V, S> for HashMap<K, V, S> {
    fn getMutWithDefault<Q: ?Sized>(&mut self, k: &Q) -> &mut V
        where
            K: Borrow<Q> + From<Q>,
            Q: Hash + Eq + Clone,
            V: Default,
    {
        if let None = self.get_mut(k) {
            self.insert(k.clone().into(), V::default());
        }
        self.get_mut(k).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::cell::UnsafeCell;
    use std::{alloc, mem, ptr};
    use std::alloc::Layout;
    use crate::utils::TrickyContainer;

    struct A {
        name: String,
    }

    impl Drop for A {
        fn drop(&mut self) {
            println!("drop {}", self.name)
        }
    }

    #[test]
    pub fn testMem() {
        let unsafeCell = UnsafeCell::new(A {
            name: "before".to_string(),
        });

        let ptr = unsafeCell.get();
        mem::forget(unsafeCell);

        unsafe {
            // ptr::drop_in_place(ptr); // 会导致程序结束的时候这块内存上的string对应
            let old = ptr::replace(ptr, A { name: "after".to_string() });
            //  alloc::dealloc(ptr as *mut u8, Layout::new::<A>()); // 对导致double free 因为UnsafeCell本身的内存当程序结束的时候会释放
            println!("print {}", old.name);
            ptr::drop_in_place(ptr);
            // alloc::dealloc(ptr as *mut u8, Layout::new::<A>()); // ptr是stack上 不能使用dealloc
        }

        println!("end");
    }

    #[test]
    pub fn testManualPtrAlloc() {
        struct Wrapper<T> {
            data: T,
        }

        unsafe {
            // malloc
            let ptr = alloc::alloc_zeroed(Layout::new::<Wrapper<A>>());

            // 写入到对应的内存ptr
            ptr::write::<Wrapper<A>>(ptr as *mut Wrapper<A>, Wrapper { data: A { name: "a".to_string() } });

            // 变换
            let reference: &Wrapper<A> = mem::transmute(ptr);

            let wrapper = &*reference;
            println!("print {}", wrapper.data.name);

            // 调用destructor
            ptr::drop_in_place(ptr as *mut Wrapper<A>);

            alloc::dealloc(ptr, Layout::new::<Wrapper<A>>())

            // Wrapper{data:A { name: "a".to_string() }};
        }
    }

    #[test]
    pub fn testTrickyContainer() {
        let dangerouCell: TrickyContainer<A> = TrickyContainer::new();

        dangerouCell.set(A { name: "test".to_string() });
        println!("{}", dangerouCell.name);

        dangerouCell.set(A { name: "test1".to_string() });
        println!("{}", &*dangerouCell.name);
    }

    #[test]
    pub fn testSort() {
        let mut vec = vec![0, 7, 1];
        vec.sort_by(|a, b| b.cmp(&a));
        println!("{:?}", vec);
    }
}