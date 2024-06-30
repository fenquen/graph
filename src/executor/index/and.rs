// Copyright (c) 2024 fenquen(https://github.com/fenquen) licensed under Apache 2.0
use std::fs::read;
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
                        // like 'abd' and like '%bd%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }

                        // like 'abd' and like '%aabda%' 矛盾
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
                        // like 'a%' and like '%a' 虽然不能融合 然而 也不矛盾
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
                    (LikePattern::EndWith(string), LikePattern::Contain(targetString)) => {
                        // like '%abd' and like '%b%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like '%a' and like '%b%' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), LikePattern::Equal(targetString)) => {
                        // like '%a%' and like 'ab'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }

                        // like '%ar%' and like 'a' 矛盾
                    }
                    (LikePattern::Contain(String), LikePattern::StartWith(targetString)) => {
                        // like '%a%' and like 'abd%' 变为 like 'abd%'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),targetValue));
                        }

                        // like '%a%' and like 'b%' 不能融合也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), LikePattern::EndWith(targetString)) => {
                        // like '%a%' and like '%rar'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),targetValue));
                        }

                        // like '%a%' and like '%b' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), LikePattern::Contain(targetString)) => {
                        // like '%a%' and like '%rar%'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),targetValue));
                        }

                        // like '%rar%' and like '%a%'
                        if string.contains(targetString) {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like '%ara%' and like '%d%' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Redundant, _) | (_, LikePattern::Redundant) => return Ok(None)
                }
            }
        }
        (Op::SqlOp(SqlOp::Like), _) => { // 下边的各类情况分析的太累了,后边的(_,Op::SqlOp(SqlOp::Like)) 需要复用
            if let GraphValue::String(string) = value {
                if let LikePattern::Redundant = op::determineLikePattern(string)? {
                    return Ok(None);
                }
            }

            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let likePattern = op::determineLikePattern(string)?;

                match (&likePattern, targetOp) {
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        if string == targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like 'ar' and >'a'
                        // like 'r' and >'a'
                        if string > targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'a' and >'a' 矛盾
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like 'ar' and >='a'
                        if string >= targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'r' and >'a' 矛盾
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like 'a' and <'b'
                        if string < targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'a' and <'a' 矛盾
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like 'a' and <='b'
                        if string <= targetString {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                        }
                        // like 'r' and <='a' 矛盾
                    }
                    // ---------------------------------------------------------------------------
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like 'a%' and ='abcd'
                        if targetString.starts_with(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
                        }
                    }
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like 'r%' and >'a'
                        // like 'aa%' and >'a'
                        if string > targetString {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like 'a%' and >'aa' 不能融合 也不矛盾
                        // like 'a%' and >'a' 不能融合 也不矛盾
                        // 例如 'a'满足'a%' 不满足>'a', 而 'ab' 满足 'a%' 且 'a%'
                        if targetString.starts_with(string) {
                            return ok_some_vec!((op,value),(targetOp,targetValue));
                        }
                        // like 'a%' and >'b' 矛盾
                    }
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like 'rd%' and >='a'
                        // like 'a%' and >='a'
                        if string >= targetString {
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }

                        // like 'a%' and >='aa' 不能融合 也不矛盾
                        // 例如 'a'满足'a%' 不满足>='aa', 而 'ab' 满足 'a%' 且 >='aa'
                        if targetString.starts_with(string) {
                            return ok_some_vec!((op,value),(targetOp,targetValue));
                        }
                        // like 'a%' and >='b' 矛盾
                    }
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        if string < targetString {
                            // like 'a%' and <'arra' 变为 <'arra'
                            if targetString.starts_with(string) {
                                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::LessThan),targetValue));
                            }

                            // like 'a%' and <'ra' 变为 like 'a%'
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }
                        // like 'a%' and <'a' 矛盾
                        // like 'r%' and <'d' 矛盾
                    }
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        if string <= targetString {
                            // like 'a%' and <='a'
                            if string == targetString {
                                return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),value));
                            }

                            // like 'a%' and <='ar' 不能融合 也不矛盾
                            // 例如 'ay'满足'a%' 不满足<='ar', 'a' 满足 'a%' 且 <='ar'
                            if targetString.starts_with(string) {
                                return ok_some_vec!((op,value),(targetOp,targetValue));
                            }

                            // like 'a%' and <='ba'
                            return ok_some_vec!((Op::SqlOp(SqlOp::Like),value));
                        }
                        // like 'r%' and <='d' 矛盾
                    }
                    // ----------------------------------------------------------------------------
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like '%r' and ='ar'
                        if targetString.ends_with(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }
                    }
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like '%a' and >'a' 不能融合 也不矛盾
                        // like '%ra' and >'a' 不能融合 也不矛盾
                        // like '%a' and >'r' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like '%a' and >='a' 不能融合 也不矛盾
                        // like '%ra' and >='a' 不能融合 也不矛盾
                        // like '%a' and >='r' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like '%r' and <'r' 不能融合 也不矛盾
                        // like '%ar' and <'r' 不能融合 也不矛盾
                        // like '%r' and <'a' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like '%r' and <='r' 不能融合 也不矛盾
                        // like '%ar' and <='r' 不能融合 也不矛盾
                        // like '%r' and <='a' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    // --------------------------------------------------------------------------
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like '%r%' and ='ara' 变为 ='ara'
                        if targetString.contains(string) {
                            return ok_some_vec!((Op::MathCmpOp(MathCmpOp::Equal),targetValue));
                        }
                        // like '%r%' and ='abcd' 矛盾
                        // like '%ar%' and ='dr' 矛盾
                    }
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like '%r%' and >'r' 不能融合 也不矛盾
                        // like '%ra%' and >'r' 不能融合 也不矛盾
                        // like '%d%' and >'r' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like '%r%' and >='r' 不能融合 也不矛盾
                        // like '%ra%' and >='r' 不能融合 也不矛盾
                        // like '%d%' and >='r' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like '%r%' and <'r' 不能融合 也不矛盾
                        // like '%ar%' and <'r' 不能融合 也不矛盾
                        // like '%r%' and <'d' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like '%r%' and <='r' 不能融合 也不矛盾
                        // like '%ar%' and <='r' 不能融合 也不矛盾
                        // like '%r%' and <='d' 不能融合 也不矛盾
                        return ok_some_vec!((op,value),(targetOp,targetValue));
                    }
                    _ => panic!("impossible")
                }
            }
        }
        (_, Op::SqlOp(SqlOp::Like)) => {
            // 和上边的逻辑是相同的只不过换了位置 利用递归两边的参数位置调换使用现有的能力
            return andWithSingle(targetOp, targetValue, op, value);
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