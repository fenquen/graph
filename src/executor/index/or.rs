use crate::graph_value::GraphValue;
use crate::parser::op::{MathCmpOp, Op};

/// 如果能融合的话 得到的vec的len是1 不然是2
/// 融合是相当有必要的 不然后续index搜索的时候会重复的
fn orWithSingle<'a>(op: Op, value: &'a GraphValue,
                    targetOp: Op, targetValue: &'a GraphValue) -> Option<Vec<(Op, &'a GraphValue)>> {
    assert!(op.permitByIndex());
    assert!(value.isConstant());

    assert!(targetOp.permitByIndex());
    assert!(targetValue.isConstant());

    match (op, targetOp) {
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue {
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value)])
            } else {
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value), (Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // =6 or >=6
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            } else {  // =6 or >=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value == targetValue { // =6 or >6
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)])
            } else if value > targetValue { // =6 or >5
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            } else { // =6 or >7
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // =6 or <=6, =6 or <=9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            } else { // =6 or <=0
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value < targetValue { // =6 or <7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            } else if value == targetValue { // =6 or <6
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            } else { //  =6 or <0
                Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // >6 or =6
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            } else if value > targetValue { // >=6 or =3
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            } else { // >6 or =9
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // >6 or >6 , >6 or >3
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            } else { // >6 or >7
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >6 and >=6, >6 and >=3
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            } else { // >3 and >=4
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >6 or <=6 , >6 or <=7 是废话
                None
            } else { // >6 or <=3
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >3 or <3 等效not equal, >3 or <4 是废话
                None
            } else { // >3 or <0
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value <= targetValue { // >=6 or =6, >=6 or =9
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)])
            } else { // >=6 or =3
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >=6 or >=6 , >=6 or >=0
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            } else { // >=6 or >=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value <= targetValue { // >=6 or > 6, >=6 or >7
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)])
            } else { // >=6 or >0
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >=6 or <=6, >=6 or <=9 废话
                None
            } else { // >=6 or <=0
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >=6 or <6, >=6 or <7 废话
                None
            } else { // >=6 or <5
                Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value >= targetValue { // <=6 or =6 , <=6 or =5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            } else { // <=6 or =9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <=6 and >6, <=6 or >5 废话
                None
            } else {  // <=6 and >9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <=6 or >=6 ,<=6 or >=0 废话
                None
            } else {  // <=6 and >=9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <=6 or <=6 ,<=6 or <=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            } else { // <=6 or <=0
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value >= targetValue { // <=6 or <6 ,<=6 or <0
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            } else {  // <=6 or <9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            }
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // <6 or =6
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)])
            } else if value > targetValue {  // <6 or =0
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)])
            } else { // <6 or =9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::Equal), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <6 or >6 等效not equal, <6 or >3 废话
                None
            } else { // <6 or >9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <6 or >=6, <6 or >=5 废话
                None
            } else { // <6 or >=9
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <6 or <=6, <6 or <=7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)])
            } else { // <6 or <=5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)])
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // <6 or <6 , <6 or <7
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)])
            } else {  // <6 or <5
                Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)])
            }
        }
        _ => panic!("impossible")
    }
}

pub(in crate::executor) fn orWithAccumulated<'a>(op: Op, value: &'a GraphValue,
                                                 previousAccumulated: Vec<(Op, &'a GraphValue)>) -> (Option<Vec<(Op, &'a GraphValue)>>, bool) {
    let mut merged = false;

    // 第1趟
    if previousAccumulated.is_empty() {
        return (Some(vec![(op, value)]), merged);
    }

    let mut accumulated = Vec::with_capacity(previousAccumulated.len());

    for (previousOp, previousValue) in previousAccumulated {
        if merged {
            accumulated.push((previousOp, previousValue));
            continue;
        }

        match orWithSingle(op, value, previousOp, previousValue) {
            None => return (None, merged), // 说明有 a<0 or a>=0 类似的废话出现了
            Some(orResult) => {
                if orResult.len() == 1 { // 说明能融合
                    accumulated.push(orResult[0]);
                    merged = true;
                } else {
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