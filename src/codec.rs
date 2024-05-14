use bytes::{Buf, Bytes, BytesMut};
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
        db_opts.set_max_write_buffer_number(1);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        {
            let db:OptimisticTransactionDB = OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();

            db.cf_handle("cf1").unwrap();
            let tx = db.transaction();

            tx.put_cf( &db.cf_handle("cf1").unwrap(), &[1][..], &[0][..]).unwrap();
            tx.commit().unwrap();

            db.create_cf("cf7",&Options::default()).unwrap();
            db.put_cf( &db.cf_handle("cf7").unwrap(), &[1][..], &[0][..]).unwrap();
        }

    }
}