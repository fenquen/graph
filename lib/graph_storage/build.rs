#![allow(non_snake_case)]

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let destPath = {
        let out_dir = env::var("OUT_DIR").unwrap();
        Path::new(&out_dir).join("os_page_size.rs")
    };

    let mut f = File::create(&destPath).unwrap();
    writeln!(f, "pub(crate) const OS_PAGE_SIZE: usize = {};", getOsPageSize()).unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}

fn getOsPageSize() -> usize {
    let osPageSize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };

    if osPageSize <= 0 {
        4096
    } else {
        osPageSize as usize
    }
}