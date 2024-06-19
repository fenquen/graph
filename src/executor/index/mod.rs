pub(super) mod or;
pub(super) mod and;

use serde_json::Value;
pub(super) use and::andWithAccumulated;
pub(super) use or::orWithAccumulated;
use crate::graph_value::GraphValue;
use crate::parser::op::Op;
use Logical::And;
use Logical::Or;
use crate::suffix_plus_plus;

#[derive(Clone, Copy)]
pub enum Logical {
    Or,
    And,
}

pub fn aa(selfOpValueVec: &[(Op, GraphValue)], selfLogical: Logical,
          betweenLogical: Logical,
          targetOpValueVec: &[(Op, GraphValue)], targerLogical: Logical) {
    let selfOpValueVec = match a(selfOpValueVec, selfLogical) {
        Some(selfOpValueVec) => selfOpValueVec,
        None => return
    };

    let targetOpValueVec = match a(targetOpValueVec, targerLogical) {
        Some(targetOpValueVec) => targetOpValueVec,
        None => return
    };

    match (selfLogical, betweenLogical, targerLogical) {
        (Or, Or, Or) => {}
        (Or, Or, And) => { // (a or b) or (c and d) => (a and c and d) or (b and c and d)
            for (selfOp, selfValue) in selfOpValueVec {}
        }
        (Or, And, And) => {}
        (Or, And, Or) => {}
        (And, And, And) => {}
        (And, And, Or) => {}
        (And, Or, And) => { // (a and c and d) or (b and c and d)
            // 可以了
        }
        (And, Or, Or) => {}
    }
}

fn a(opValueVec: &[(Op, GraphValue)], logical: Logical) -> Option<Vec<(Op, &GraphValue)>> {
    let mut selfAccumulated = Vec::new();

    for (selfOp, selfValue) in opValueVec {
        let (selfAccumulatedNew, _) = match logical {
            Logical::Or => {
                orWithAccumulated(*selfOp, selfValue, selfAccumulated)
            }
            Logical::And => {
                andWithAccumulated(*selfOp, selfValue, selfAccumulated)
            }
        };

        match selfAccumulatedNew {
            Some(selfAccumulatedNew) => selfAccumulated = selfAccumulatedNew,
            None => {
                return None;
            }
        }
    }

    Some(selfAccumulated)
}

#[derive(Default)]
pub struct AndDesc<'a> {
    pub parent: Option<&'a AndDesc<'a>>,
    pub op: Option<Op>,
    pub value: Option<GraphValue>,
   // pub children: Vec<Box<AndDesc<'a>>>,
}

#[derive(Default)]
pub struct VirtualSlice<'a, T> {
    pub content: Vec<&'a [T]>,
    currentVecIndex: usize,
    currentIndex: usize,
}

impl<'a, T> Iterator for VirtualSlice<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.content.get(self.currentVecIndex) {
            Some(&slice) => {
                match slice.get(self.currentIndex) {
                    Some(t) => {
                        suffix_plus_plus!(self.currentIndex);
                        Some(t)
                    }
                    None => {
                        suffix_plus_plus!(self.currentVecIndex);
                        self.currentIndex = 0;

                        self.next()
                    }
                }
            }
            None => None,
        }
    }
}