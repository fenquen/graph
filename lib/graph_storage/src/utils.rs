use std::io;
use std::ops::{BitAnd, Sub};
use anyhow::Result;

pub(crate) fn getOsPageSize() -> u16 {
    invokeLibcFn(|| { unsafe { libc::sysconf(libc::_SC_PAGESIZE) } }).map_or_else(
        |_| { DEFAULT_PAGE_SIZE },
        |pageSize| { pageSize as u16 },
    )
}

pub(crate) fn invokeLibcFn<T: LibcResult>(func: impl Fn() -> T) -> Result<T> {
    let t = func();

    if t.success() {
        Ok(t)
    } else {
        throw!(t.errMsg())
    }
}

pub(crate) trait LibcResult: Sized + Copy {
    fn success(&self) -> bool;

    fn errMsg(&self) -> String {
        if self.success() {
            return EMPTY_STR.to_string();
        }

        io::Error::last_os_error().to_string()
    }
}

impl LibcResult for i64 {
    fn success(&self) -> bool {
        *self >= 0
    }
}

pub(crate) fn isPowerOfTwo<T>(n: T) -> bool
where
    T: BitAnd<Output=T> + Sub<Output=T> + PartialEq + From<u8> + Copy,
{
    n != T::from(0) && (n & (n - T::from(1))) == T::from(0)
}

pub(crate) const EMPTY_STR: &str = "";
pub(crate) const DEFAULT_PAGE_SIZE: u16 = 4096;