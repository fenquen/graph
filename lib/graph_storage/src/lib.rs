#![allow(non_snake_case)]

pub mod db;
mod utils;

#[cfg(test)]
mod tests {
    use crate::utils;

    #[test]
    fn general() {
        println!("{}", utils::getPageSize());
    }
}
