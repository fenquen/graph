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
    ($file:expr) => {
         $file.seek(std::io::SeekFrom::End(0)).await?
    };
}