// #[macro_export] // 宏导出到当前 crate 之外
macro_rules! throw {
    ($a:expr) => {
        core::result::Result::Err(anyhow::Error::msg($a))?
    };
}
