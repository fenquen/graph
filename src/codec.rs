use std::alloc::{Allocator, Global};
use bytes::{Buf, Bytes, BytesMut};
use anyhow::Result;
use bumpalo::Bump;
use crate::executor::CommandExecutor;
use crate::graph_error::GraphError;
use crate::graph_value::GraphValue;
use crate::types::{Byte, SessionVec};

pub trait BinaryCodec<'vecAllocator> {
    type OutputType;

    fn encode2ByteMut(&self, destByteSlice: &mut BytesMut) -> Result<()>;

    fn decodeFromSliceWrapper<'commandExecutor: 'vecAllocator>(srcSliceWrapper: &mut SliceWrapper,
                                                               executor: Option<&'commandExecutor CommandExecutor>) -> Result<Self::OutputType>;
    /// 返回写入的byte数量
    fn encode2Slice(&self, destByteSlice: &mut [Byte]) -> Result<usize>;
}

impl<'vecAllocator, T: BinaryCodec<'vecAllocator, OutputType=T>> BinaryCodec<'vecAllocator> for Vec<T> {
    type OutputType = Vec<T>;

    fn encode2ByteMut(&self, destByteSlice: &mut BytesMut) -> Result<()> {
        for t in self {
            t.encode2ByteMut(destByteSlice)?;
        }

        Ok(())
    }

    fn decodeFromSliceWrapper<'commandExecutor: 'vecAllocator>(srcSliceWrapper: &mut SliceWrapper,
                                                               _: Option<&'commandExecutor CommandExecutor>) -> Result<Self::OutputType> {
        let mut vec = vec![];

        loop {
            if srcSliceWrapper.remaining() == 0 {
                break;
            }

            vec.push(T::decodeFromSliceWrapper(srcSliceWrapper, None)?);
        }

        Ok(vec)
    }

    fn encode2Slice(&self, destByteSlice: &mut [Byte]) -> Result<usize> {
        let mut totalWriteCount = 0usize;

        for t in self {
            let writeCount = t.encode2Slice(destByteSlice)?;
            totalWriteCount += writeCount;
        }

        assert_eq!(destByteSlice.len(), totalWriteCount);

        Ok(totalWriteCount)
    }
}

impl<'vecAllocator, T: BinaryCodec<'vecAllocator, OutputType=T>> BinaryCodec<'vecAllocator> for SessionVec<'vecAllocator, T> {
    type OutputType = SessionVec<'vecAllocator, T>;

    fn encode2ByteMut(&self, destByteSlice: &mut BytesMut) -> Result<()> {
        for t in self {
            t.encode2ByteMut(destByteSlice)?;
        }

        Ok(())
    }

    fn decodeFromSliceWrapper<'commandExecutor: 'vecAllocator>(srcSliceWrapper: &mut SliceWrapper,
                                                               executor: Option<&'commandExecutor CommandExecutor>) -> Result<Self::OutputType> {
        let executor = executor.unwrap();
        let mut vec: SessionVec<'vecAllocator, T> = executor.vecNewIn();

        loop {
            if srcSliceWrapper.remaining() == 0 {
                break;
            }

            vec.push(T::decodeFromSliceWrapper(srcSliceWrapper, None)?);
        }

        Ok(vec)
    }

    fn encode2Slice(&self, destByteSlice: &mut [Byte]) -> Result<usize> {
        todo!()
    }
}

pub struct MyBytes {
    pub bytes: Bytes,
    /// Bytes的len()函数相当的坑 其实是remaining()
    pub len: usize,
}

impl MyBytes {
    /// bytes尚未提供position()
    pub fn position(&self) -> usize {
        self.len - self.bytes.remaining() - 1
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

pub struct SliceWrapper<'a> {
    pub slice: &'a [Byte],
    pub position: usize,
    pub len: usize,
}

impl<'a> Buf for SliceWrapper<'a> {
    fn remaining(&self) -> usize {
        self.len - self.position
    }

    fn chunk(&self) -> &[u8] {
        &self.slice[self.position..]
    }

    fn advance(&mut self, cnt: usize) {
        self.position += cnt;
    }
}

impl<'a> SliceWrapper<'a> {
    pub fn new(slice: &'a [Byte]) -> SliceWrapper<'a> {
        SliceWrapper {
            slice,
            position: 0,
            len: slice.len(),
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};
    use rocksdb::{IteratorMode, OptimisticTransactionDB};

    #[test]
    fn testRocksDb() {
        use rocksdb::{DB, ColumnFamilyDescriptor, Options};

        let path = "rocksdb";

        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

        let mut db_opts = Options::default();
        db_opts.set_keep_log_file_num(1);
        db_opts.set_max_write_buffer_number(1);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        {
            let db: OptimisticTransactionDB = OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();

            let tx = db.transaction();

            // let iterator = tx.iterator(IteratorMode::Start);

            tx.put_cf(&db.cf_handle("cf1").unwrap(), &[1][..], &[0][..]).unwrap();
            tx.commit().unwrap();

            db.create_cf("cf7", &Options::default()).unwrap();
            db.put_cf(&db.cf_handle("cf7").unwrap(), &[1][..], &[0][..]).unwrap();
        }
    }
}