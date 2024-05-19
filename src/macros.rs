use std::error::Error;

#[macro_export]
macro_rules! throw {
    ($a:expr) => {
        core::result::Result::Err(crate::graph_error::GraphError::new($a))?
    };
}

#[macro_export]
macro_rules! prefix_plus_plus {
    ($expr:expr) => {
        {
            $expr = $expr + 1;
            $expr
        }
    };
}

#[macro_export]
macro_rules! prefix_minus_minus {
    ($expr:expr) => {
        {
            $expr = $expr - 1;
            $expr
        }
    };
}

#[macro_export]
macro_rules! suffix_plus_plus {
    ($expr:expr) => {
        {
            let old = $expr;
            $expr = $expr + 1;
            old
        }
    };
}

#[macro_export]
macro_rules! suffix_minus_minus {
    ($expr:expr) => {
        {
            let old = $expr;
            $expr = $expr - 1;
            old
        }
    };
}

#[macro_export]
macro_rules! file_goto_start {
    ($file:expr) => {
         $file.seek(std::io::SeekFrom::Start(0)).await?
    };
}

#[macro_export]
macro_rules! file_goto_end {
    ($file: expr) => {
         $file.seek(std::io::SeekFrom::End(0)).await?
    };
}

#[macro_export]
macro_rules! u64_to_byte_array_reference {
    ($u64: expr) => {
        &[
            ($u64 >> 56) as u8,
            ($u64 >> 48) as u8,
            ($u64 >> 48) as u8,
            ($u64 >> 32) as u8,
            ($u64 >> 24) as u8,
            ($u64 >> 16) as u8,
            ($u64 >> 8) as u8,
            $u64 as u8 ]
    };
}

#[macro_export]
macro_rules! byte_slice_to_u64 {
    ($slice: expr) => {
        (($slice[0] as u64) << 56) |
        (($slice[2] as u64) << 48) |
        (($slice[3] as u64)<< 32)  |
        (($slice[4] as u64)<< 24)  |
        (($slice[5] as u64)<< 16)  |
        (($slice[6] as u64)<< 8)   |
        ($slice[7] as u64)
    };
}