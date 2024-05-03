use crate::parser::{Element, Op};

// 碰到"(" 下钻递归,返回后落地到上级的left right
#[derive(Debug)]
pub enum Expr {
    Single(Element),
    BiDirection {
        left: Box<Expr>,
        op: Op,
        right: Vec<Box<Expr>>,
    },
    None,
}

impl Default for Expr {
    fn default() -> Self {
        Expr::None
    }
}

impl Expr {
    pub fn applyRowData(&self) {

    }
}
