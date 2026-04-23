use std::error::Error;

#[macro_export]
macro_rules! throw {
    ($a:expr) => {
        core::result::Result::Err(crate::graph_error::GraphError::new($a))?
    };
}

#[macro_export]
macro_rules! throwFormat {
    ($($a:tt)*) => {
        core::result::Result::Err(crate::graph_error::GraphError::new(&format!($($a)*)))?
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
macro_rules! u64ToByteArrRef {
    ($u64: expr) => {
        &u64::to_be_bytes($u64)
        // &[
        //     ($u64 >> 56) as u8,
        //     ($u64 >> 48) as u8,
        //     ($u64 >> 48) as u8,
        //     ($u64 >> 32) as u8,
        //     ($u64 >> 24) as u8,
        //     ($u64 >> 16) as u8,
        //     ($u64 >> 8) as u8,
        //     $u64 as u8 ]
    };
}

/// 使用 u64::from_be_bytes(byteSlice)
#[macro_export]
macro_rules! byte_slice_to_u64 {
    ($slice: expr) => {
        {
            assert!($slice.len() >= 8, "slice需要至少8字节");
            u64::from_be_bytes(unsafe { $slice.as_ptr().cast::<[u8; 8]>().read()})
        }
    };
}

#[macro_export]
macro_rules! byte_slice_to_u32 {
    ($slice: expr) => {
        {
            assert!($slice.len() >= 4, "slice至少需要4字节");
            // u32::from_be_bytes(unsafe { $slice.as_ptr().cast::<[u8; 4]>().read()})
            (($slice[0] as u32)<< 24)  |
            (($slice[1] as u32)<< 16)  |
            (($slice[2] as u32)<< 8)   |
            ($slice[3] as u32)
        }
    };
}