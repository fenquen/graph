use crate::graph_value::GraphValue;
use crate::parser::op::{MathCmpOp, Op};

fn andWithSingle<'a>(op: Op, value: &'a GraphValue,
                     targetOp: Op, targetValue: &'a GraphValue) -> Option<Vec<(Op, &'a GraphValue)>> {
    assert!(op.permitByIndex());
    assert!(value.isConstant());

    assert!(targetOp.permitByIndex());
    assert!(targetValue.isConstant());

    match (op, targetOp) {
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue {
                return Some(vec![(op, value)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue {
                return Some(vec![(op, value)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value > targetValue {
                return Some(vec![(op, value)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue {
                return Some(vec![(op, value)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value < targetValue {
                return Some(vec![(op, value)]);
            }

            None
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if targetValue > value {
                return Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), targetValue)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            } else {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >6 and >=6
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            } else { // >3 and >=4
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value == targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            } else if value > targetValue { // >=6 and <=3
                None
            } else { // >=6 and <=9
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value >= targetValue { // >=3 and <3
                None
            } else { // >=3 and <4
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if targetValue >= value {
                return Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), targetValue)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)])
            } else {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value > targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)])
            } else {
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value == targetValue { // >=6 and <=6
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value)])
            } else if value > targetValue { // >=6 and <=0
                None
            } else { // >=6 and <=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value == targetValue { // >=6 and <6
                None
            } else if value > targetValue { // >=7 and <6
                None
            } else { // >=6 and <7
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if targetValue <= value {
                return Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), targetValue)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value > targetValue { // <=6 and >3
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            } else {  // <=6 and >6
                None
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value == targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value)])
            } else if value > targetValue {  // <=6 and >5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            } else { // <=6 and >7
                None
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            } else {
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value >= targetValue { // <=6 and <6
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            } else {  // <=6 and <7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            }
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if targetValue < value {
                return Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), targetValue)]);
            }

            None
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value > targetValue { // <6 and >5
                return Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)]);
            }

            // <6 and >6
            None
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value > targetValue { // <6 and >=5
                return Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)]);
            }

            // <6 and >=6  <6 and >=7
            None
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <6 and <=6, <6 and <=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)])
            } else { // <6 and <=5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // <6 and <6 , <6 and <7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)])
            } else {  // <6 and <5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        _ => panic!("impossible")
    }
}

pub(in crate::executor) fn andWithAccumulated<'a>(op: Op, value: &'a GraphValue,
                                                  previousAccumulated: Vec<(Op, &'a GraphValue)>) -> (Option<Vec<(Op, &'a GraphValue)>>, bool) {
    let mut merged = false;

    if previousAccumulated.is_empty() {
        return (Some(vec![(op, value)]), merged);
    }

    let mut accumulated = Vec::new();

    // 原先是 [6,16] 来了 a<=7,a>9
    for (previousOp, previousValue) in previousAccumulated {
        if merged {
            accumulated.push((previousOp, previousValue));
        }

        match andWithSingle(op, value, previousOp, previousValue) {
            None => return (None, merged), // 说明有 a>0 and a<=0
            Some(andResult) => {
                if andResult.len() == 1 { // 说明能融合
                    accumulated.push(andResult[0]);
                    merged = true;
                } else { // 不能融合 还是原样到vec
                    accumulated.push((previousOp, previousValue));
                }
            }
        }
    }

    if merged == false {
        accumulated.push((op, value));
    }

    (Some(accumulated), merged)
}