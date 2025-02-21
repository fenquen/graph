use std::{fs, io};
use std::fs::{Metadata};
use std::ops::{BitAnd, Sub};
use std::os::fd::{RawFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use anyhow::Result;
use memmap2::{Mmap, MmapOptions};
use crate::constant;
use crate::types::TxId;

pub(crate) const EMPTY_STR: &str = "";
pub(crate) const DEFAULT_PAGE_SIZE: u16 = 4096;

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
            EMPTY_STR.to_string()
        } else {
            io::Error::last_os_error().to_string()
        }
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

pub(crate) fn recursiveSymbolic(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();

    // fs::symlink_metadata(&dbOption.dirPath)?; // 只会读取symbolic本身不会深入
    // 如果是symbolic,会深入1步 不会无限深入
    let metadata = fs::metadata(path)?;

    if metadata.is_symlink() {
        let targetPath = fs::read_link(path)?;
        recursiveSymbolic(targetPath)
    } else {
        Ok(path.to_str().unwrap().to_string())
    }
}

pub(crate) fn haveWritePermission(metadata: &Metadata) -> bool {
    let currentUid = unsafe { libc::getuid() };
    let currentGid = unsafe { libc::getgid() };

    let mode = metadata.permissions().mode();

    let mut writable = false;

    // current user is owner
    if currentUid == metadata.uid() {
        if (mode & 0o200) != 0 {
            writable = true;
        }
    }

    // current user is in owner group
    if currentGid == metadata.gid() {
        if (mode & 0o020) != 0 {
            writable = true;
        }
    }

    if (mode & 0o002) == 0 {
        writable = true;
    }

    writable
}

#[inline]
pub(crate) fn mmapFd(fd: RawFd, offset: u64, len: usize) -> Result<Mmap> {
    unsafe { Ok(MmapOptions::new().offset(offset).len(len).map(fd)?) }
}

pub(crate) unsafe fn slice2Ref<'a, T>(slice: impl AsRef<[u8]>) -> &'a T {
    let slice = slice.as_ref();
    &*(slice.as_ptr() as *const T)
}

pub(crate) unsafe fn slice2RefMut<'a, T>(slice: impl AsRef<[u8]>) -> &'a mut T {
    let slice = slice.as_ref();
    &mut *(slice.as_ptr() as *mut T)
}

pub(crate) fn slice2ArrayRef<const N: usize>(slice: &[u8]) -> Option<&[u8; N]> {
    if slice.len() == N {
        let arr_ref: &[u8; N] = unsafe { &*(slice.as_ptr() as *const [u8; N]) };
        Some(arr_ref)
    } else {
        None
    }
}

pub(crate) fn appendKeyWithTxId(key: &[u8], txId: TxId) -> Vec<u8> {
    let mut keyWithTxId = Vec::with_capacity(key.len() + constant::TX_ID_SIZE);
    keyWithTxId.extend_from_slice(&key[..]);
    keyWithTxId.extend_from_slice(txId.to_be_bytes().as_ref());
    keyWithTxId
}