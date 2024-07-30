#![feature(allocator_api)]
#![allow(non_snake_case)]

use std::alloc::{Allocator, Global, System};
use bumpalo::Bump;

pub trait AllocatorExt: Allocator {
    fn custom() -> bool {
        false
    }
}

impl AllocatorExt for Global {}

impl AllocatorExt for System {}

impl AllocatorExt for &Bump {
    fn custom() -> bool {
        true
    }
}

