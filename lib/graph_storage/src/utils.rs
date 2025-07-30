use std::{fs, io};
use std::fs::{Metadata};
use std::ops::{Add, BitAnd, Rem, Sub};
use std::os::fd::{RawFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::str::FromStr;
use anyhow::Result;
use memmap2::{Mmap, MmapMut, MmapOptions};
use crate::constant;

pub(crate) const EMPTY_STR: &str = "";
pub(crate) const DEFAULT_PAGE_SIZE: usize = 4096;

pub(crate) fn getOsPageSize() -> usize {
    invokeLibcFn(|| { unsafe { libc::sysconf(libc::_SC_PAGESIZE) } }).map_or_else(
        |_| { DEFAULT_PAGE_SIZE },
        |pageSize| { pageSize as usize },
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

#[inline]
pub(crate) fn mmapFdMut(fd: RawFd, offset: Option<u64>, len: Option<usize>) -> Result<MmapMut> {
    unsafe {
        let mut mmapOptions = MmapOptions::new();

        if let Some(offset) = offset {
            mmapOptions.offset(offset);
        }

        if let Some(len) = len {
            mmapOptions.len(len);
        }

        Ok(mmapOptions.map_mut(fd)?)
    }
}

pub(crate) fn slice2Ref<'a, T>(slice: impl AsRef<[u8]>) -> &'a T {
    unsafe {
        let slice = slice.as_ref();

        // slice对应的指针位置可能不是align的倍数,如果化为引用的话会panic的
        let actual = alignUp(slice.as_ptr() as usize, align_of::<T>());

        &*(actual as *const T)
    }
}

pub(crate) fn slice2RefMut<'a, T>(slice: impl AsRef<[u8]>) -> &'a mut T {
    unsafe {
        let slice = slice.as_ref();

        // slice对应的指针位置可能不是align的倍数,如果化为引用的话会panic的
        let actual = alignUp(slice.as_ptr() as usize, align_of::<T>());

        &mut *(actual as *mut T)
    }
}

pub(crate) fn slice2ArrayRef<const N: usize>(slice: &[u8]) -> Option<&[u8; N]> {
    if slice.len() == N {
        let arr_ref: &[u8; N] = unsafe { &*(slice.as_ptr() as *const [u8; N]) };
        Some(arr_ref)
    } else {
        None
    }
}

pub(crate) fn extractFileNum(path: impl AsRef<Path>) -> Option<usize> {
    let fileName = path.as_ref().file_name().unwrap().to_str().unwrap();
    let elemVec = fileName.split(constant::DOT_STR).collect::<Vec<&str>>();
    if 1 >= elemVec.len() {
        return None;
    }

    usize::from_str(elemVec.get(0).unwrap()).ok()
}

#[inline]
pub(crate) const fn alignUp(ptr: usize, align: usize) -> usize {
    (ptr + align - 1) & !(align - 1)
}

pub(crate) fn roundUp2Multiple<T>(value: T, multiple: T) -> T
where
    T: Rem<Output=T> + Add<Output=T> + Sub<Output=T> + Default + Copy + PartialEq,
{
    let remainder = value.rem(multiple);

    if remainder != T::default() {
        value + (multiple - remainder)
    } else {
        value
    }
}