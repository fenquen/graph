#![allow(non_snake_case)]
#![allow(unused)]

pub mod db;
mod utils;

#[cfg(test)]
mod tests {
    use crate::db::DB;
    use crate::utils;

    #[test]
    fn general() {
        DB::open("graph.db").unwrap();
    }
}
