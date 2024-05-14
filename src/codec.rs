use bytes::{Buf, Bytes, BytesMut};
use crate::global::Byte;
use anyhow::Result;

pub trait BinaryCodec {
    type OutputType;

    /// 额外传递srcByteSlice的长度 <br>
    /// 因为读取string的时候不想copy_to_slice()产生copy 想直接对srcByteSlice切片 <br>
    /// 然而Bytes不提供position 且它的len()函数相当的坑 其实是remaining()
    fn decode(srcByteSlice: &mut MyBytes) -> Result<Self::OutputType>;

    fn encode(&self, destByteSlice: &mut BytesMut) -> Result<()>;
}

pub struct MyBytes {
    pub bytes: Bytes,
    /// Bytes的len()函数相当的坑 其实是remaining()
    pub len: usize,
}

impl MyBytes {
    /// bytes尚未提供position()
    pub fn position(&self) -> usize {
        self.len - self.bytes.remaining()
    }
}

impl From<Bytes> for MyBytes {
    fn from(bytes: Bytes) -> Self {
        MyBytes {
            len: bytes.remaining(),
            bytes,
        }
    }
}