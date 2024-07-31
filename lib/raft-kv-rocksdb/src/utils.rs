pub fn id2ByteVec(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

pub fn byteSlice2Id(byteSlice: &[u8]) -> u64 {
    let (slice, _) = byteSlice.split_at(size_of::<u64>());
    u64::from_be_bytes(slice.try_into().unwrap())
}