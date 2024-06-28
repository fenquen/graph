use crate::graph_value::GraphValue;
use crate::ok_some_vec;
use crate::parser::op::{LikePattern, MathCmpOp, Op, SqlOp};
use anyhow::Result;
use crate::parser::op;

pub(super) fn andWithSingle<'a>(op: Op, value: &'a GraphValue,
                                targetOp: Op, targetValue: &'a GraphValue) -> Result<Option<Vec<(Op, &'a GraphValue)>>> {
    assert!(op.permitByIndex());
    assert!(value.isConstant());

    assert!(targetOp.permitByIndex());
    assert!(targetValue.isConstant());

    match (op, targetOp) { // todo 如何应对 string的 like 'a%' 和 >='a' 的融合
        (Op::SqlOp(SqlOp::Like), Op::SqlOp(SqlOp::Like)) => {
            if value == targetValue {
                return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
            }

            if let GraphValue::String(string) = value {
                if let LikePattern::Redundant = op::determineLikePattern(string)? {
                    return Ok(None);
                }
            }

            if let GraphValue::String(targetString) = targetValue {
                if let LikePattern::Redundant = op::determineLikePattern(targetString)? {
                    return Ok(None);
                }
            }

            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let likePattern = op::determineLikePattern(string)?;
                let targetLikePattern = op::determineLikePattern(targetString)?;

                match (&likePattern, &targetLikePattern) {
                    (LikePattern::Equal(string), LikePattern::Equal(targetString)) => {
                        if string == targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }
                    }
                    (LikePattern::Equal(string), LikePattern::StartWith(targetString)) => {
                        // like 'abd' and like 'a%' 变为 ='abd'
                        if string.starts_with(targetString) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'abd' and like 'd%' 矛盾
                    }
                    (LikePattern::Equal(string), LikePattern::EndWith(targetString)) => {
                        // like 'aba' and like '%a'
                        if string.ends_with(targetString) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'abd‘ and like '%a' 矛盾
                    }
                    (LikePattern::Equal(string), LikePattern::Contain(targetString)) => {
                        // like 'abd' and like '%aabda%'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }

                        // like 'abd' and like '%bd%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }

                        // like 'ab' and like '%ba%' 矛盾
                    }
                    (LikePattern::StartWith(string), LikePattern::Equal(targetString)) => {
                        // like 'a%' and like 'abcd' 变为 ='abcd'
                        if targetString.starts_with(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }
                    }
                    (LikePattern::StartWith(string), LikePattern::StartWith(targetString)) => {
                        // like 'ab%' and like 'a%'
                        if string.starts_with(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like 'a%' and like 'ab%'
                        if targetString.starts_with(string) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),targetValue));
                        }

                        // like 'ab%' and like 'ay%' 矛盾
                    }
                    (LikePattern::StartWith(_), LikePattern::EndWith(_)) => {
                        // like 'a%' and like '%a'
                        // like 'a%' and like '%b' 虽然不能融合 然而 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::StartWith(string), LikePattern::Contain(targetString)) => {
                        // like 'abcd%' and like '%b%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like 'a%' and like '%b%' 不能融合也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::EndWith(string), LikePattern::Equal(targetString)) => {
                        // like '%a' and like 'aba'
                        if targetString.ends_with(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'abd' and like '%a' 矛盾
                    }
                    (LikePattern::EndWith(string), LikePattern::StartWith(targetString)) => {
                        // like 'a%' and like '%a'
                        // like 'a%' and like '%b' 虽然不能融合 然而 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::EndWith(string), LikePattern::EndWith(targetString)) => {
                        // like '%d' and like '%abcd'
                        if targetString.ends_with(string) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),targetValue));
                        }

                        // like '%abcd' and like '%d'
                        if string.ends_with(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }
                        // like '%abcd' and like '%ab' 矛盾
                    }
                    (LikePattern::EndWith(string),LikePattern::Contain(targetString))=>{
                        // like '%abd' and like '%b%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like '%a' and like '%b%' 不能融合 也不矛盾
                    }
                    _ => {}
                }
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::Equal)) => {
            // =0 and =0 变为 =0
            if value == targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            // =1 and >=0 变为 =1
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            // =1 and >0 变为 =1
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            // =0 and <=0 变为 =0
            if value <= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            // =0 and <1 变为 =0
            if value < targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            // >1 and =9 变为 =9
            if targetValue > value {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            // >0 and >=0 变为 >0
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
            }

            // >0 and >3 变为 >3
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            // >0 and >=0 变为 >0
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
            }

            // >3 and >=7 变为 >=7
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            // >0 and <=0 变为 =0
            if value == targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
            }

            // >0 and <=1 变为 (0,1]
            if targetValue > value {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
            }

            // >1 and <=0
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            // >0 and <1 变为 (0,1)
            if value < targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
            }

            // >=0 and <0
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            //>=0 and =3 变为 =3
            if targetValue >= value {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
            }
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            // >=3 and >=0 变为 >=3
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
            }

            // >=0 and >=3 变为 >=3
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            // >=3 and >0 变为 >=3
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
            }

            // >=0 and >3 变为 >=3
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            // >=0 and <=0 变为 =0
            if value == targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }

            // >=6 and <=7 变为 [6,7]
            if targetValue > value {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
            }

            // >=6 and <=0
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            // >=6 and <7 变为 [6,7)
            if targetValue < value {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value), (Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
            }

            // >=6 and <6 和 >=7 and <6
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            // <=1 and =0
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            // <=6 and >3 变为 (3,6]
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
            }

            // <=0 and >0 矛盾
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            // <=0 and >=0 变为 =0
            if value == targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }

            // <=1 and >0 变为 (0,1]
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
            }

            // <=0 and >1 矛盾
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            // <=0 and <=7 变为 <=7
            if value <= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
            }

            // <=1 and <=0 变为 <=1
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            // <=1 and <0 变为 <0
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
            }

            // <=1 and <7 变为 <=1
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            // <1 and =0 变为 =0
            if value >= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
            }
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            // <1 and >0 变为 (0,1)
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
            }

            // <1 and >3 矛盾
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            // <1 and >=0
            if value > targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), value), (Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
            }

            // <0 and >=0  ,<0 and >=1 矛盾
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            // <0 and <=1 变为 <0
            if value <= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), value));
            }

            // <1 and <=0 变为 <=0
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            // <0 and <1 变为 <0
            if value <= targetValue {
                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), value));
            }

            // <1 and <0 变为 <0
            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
        }
        _ => panic!("impossible")
    }

    // and 的兜底是None 矛盾
    Ok(None)
}