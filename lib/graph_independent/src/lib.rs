#![feature(allocator_api)]
#![allow(non_snake_case)]

use std::alloc::{AllocError, Allocator, Global, Layout, System};
use std::ptr::NonNull;
use bumpalo::Bump;

pub trait AllocatorExt: Allocator {
    fn custom() -> bool {
        false
    }

    fn dummy() -> bool {
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

pub struct DummyAllocator;

unsafe impl Allocator for DummyAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        panic!("Dummy Allocator")
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        panic!("Dummy Allocator")
    }
}

impl AllocatorExt for DummyAllocator {
    fn dummy() -> bool {
        true
    }
}

