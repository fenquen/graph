use crate::graph_value::GraphValue;
use crate::parser::op::{LikePattern, MathCmpOp, Op, SqlOp};
use crate::{global, ok_merged, ok_not_merged, utils};
use crate::parser::op;
use anyhow::Result;
use crate::executor::optimizer::merge::MergeResult;

/// 如果能融合的话 得到的vec的len是1 不然是2
/// 融合是相当有必要的 不然后续index搜索的时候会有很多无谓的重复 对性能有损失的
pub fn opValueOrOpValue<'a>(op: Op, value: &'a GraphValue,
                                   targetOp: Op, targetValue: &'a GraphValue) -> Result<MergeResult<'a>> {
    assert!(op.permitByIndex());
    assert!(value.isConstant());

    assert!(targetOp.permitByIndex());
    assert!(targetValue.isConstant());

    // 能保证调用本函数的时候,in 已经被消化掉了变成了多个equal
    // 压缩原则是 宁可胆小不压缩 也不要冒险的压缩融合 因为可能会筛掉原本满足条件的数据, 大不了命中率低点后边还有getDataByKey会对原表数据再校验来兜底
    match (op, targetOp) {
        // 因为like的目标数据种类有 GraphValue::Null 和 GraphValue::String
        // 还要深入GraphValue::String 来探讨,不过不这样的话 like '%' 这样的废话就漏过
        (Op::SqlOp(SqlOp::Like), Op::SqlOp(SqlOp::Like)) => {
            // 用if let因为可能会有 like null, like '%' 含有Redundant踢掉
            if let GraphValue::String(string) = value {
                if let LikePattern::Nonsense = op::determineLikePattern(string)? {
                    return Ok(MergeResult::Nonsense);
                }
            }

            // 用if let因为可能会有 like null, like '%' 含有Redundant踢掉
            if let GraphValue::String(targetString) = targetValue {
                if let LikePattern::Nonsense = op::determineLikePattern(targetString)? {
                    return Ok(MergeResult::Nonsense);
                }
            }

            // 不在乎具体的数据种类 ,不管是 like null 和 like null 还是 like '%a' 和 like '%a'
            if value == targetValue {
                return Ok(MergeResult::Merged((Op::SqlOp(SqlOp::Like), value)));
            }

            // 目前只对两边都是string的时候尝试去压缩,去掉不必要的or条件
            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let likePattern = op::determineLikePattern(string)?;
                let targetLikePattern = op::determineLikePattern(targetString)?;

                // 含有Redundant踢掉,事实上是用不着的上边的会兜底的,写了是为了后续阅读方便
                // if let LikePattern::Redundant = &likePattern {
                //     return Ok(None);
                // }
                // if let LikePattern::Redundant = &targetLikePattern {
                //     return Ok(None);
                // }

                // 因为两边都是like,这里是两边的likePattern比较
                match (&likePattern, &targetLikePattern) { // 统共要有16类情况
                    (LikePattern::Equal(string), LikePattern::StartWith(targetString)) => {
                        // like 'daa' or like 'd%' 变为 like 'd%'
                        if string.starts_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::Equal(string), LikePattern::EndWith(targetString)) => {
                        // like 'abd' or like '%d' 变为 like 'd%'
                        if string.ends_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::StartWith(string), LikePattern::Equal(targetString)) => {
                        // like 'd%' or like 'daa' 变为 like 'd%'
                        if targetString.starts_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::EndWith(string), LikePattern::Equal(targetString)) => {
                        // like '%d' or like 'abd' 变为 like 'd%'
                        if targetString.starts_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::Equal(_), _) | (_, LikePattern::Equal(_)) => {
                        let likeString = likePattern.getString()?;
                        let targetLikeString = targetLikePattern.getString()?;

                        // like 'a' or like 'a'
                        // like 'a' or like 'a%'
                        // like 'a' or like '%a'
                        // like 'a' or like '%a%'
                        if likeString == targetLikeString {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::StartWith(string), LikePattern::StartWith(targetString)) => {
                        // like 'a%' or like 'abcd%'
                        if targetString.starts_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }

                        // like 'abcd%' or like 'a%'
                        if string.starts_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::StartWith(_), LikePattern::EndWith(_)) => {
                        // like 'a%' or like '%a' ,不能融合
                    }
                    (LikePattern::StartWith(string), LikePattern::Contain(targetString)) => {
                        // like 'abcd%' or like '%b%'
                        if string.contains(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::EndWith(_), LikePattern::StartWith(_)) => {
                        // like '%a' or like 'a%' ,不能融合
                    }
                    (LikePattern::EndWith(string), LikePattern::EndWith(targetString)) => {
                        // like '%d' or like '%abcd'
                        if targetString.ends_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }

                        // like '%abcd' or like '%d'
                        if string.ends_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::EndWith(string), LikePattern::Contain(targetString)) => {
                        // like '%abcd' or like '%b%'
                        if string.contains(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::StartWith(targetString)) => {
                        // like '%a%' or like 'bacd%'
                        if targetString.contains(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::EndWith(targetString)) => {
                        // like '%a%' or like '%bacd'
                        if targetString.contains(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::Contain(targetString)) => {
                        // like '%abd%' or like '%dabdr%'
                        if targetString.contains(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }

                        // like '%dabdr' or like '%abd%'
                        if string.contains(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (LikePattern::Nonsense, _) | (_, LikePattern::Nonsense) => return Ok(MergeResult::Nonsense)
                }
            }
        }
        (Op::SqlOp(SqlOp::Like), _) => {
            // 用if let是因为可能会有like null 不是string, like '%' 含有Redundant踢掉
            if let GraphValue::String(string) = value {
                if let LikePattern::Nonsense = op::determineLikePattern(string)? {
                    return Ok(MergeResult::Nonsense);
                }
            }

            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let likePattern = op::determineLikePattern(string)?;

                //  likePattern 4种, targetOp 5类
                match (&likePattern, targetOp) {
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like 'd' or ='d'
                        if string == targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::Equal), targetValue));
                        }
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like 'd' or >'d'
                        if string == targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
                        }

                        // like 'd' or >'a'
                        if string > targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan),targetValue));
                        }
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like 'd' or >='a'
                        if string >= targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
                        }
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like 'd' or <'d'
                        if string == targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
                        }

                        //  like 'a' or <'b'
                        if string < targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
                        }
                    }
                    (LikePattern::Equal(string), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like 'a' or <='r'
                        if string <= targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
                        }
                    }
                    //-------------------------------------------------------------------------------
                    (LikePattern::StartWith(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like 'a%' or ='abcd'
                        if targetString.starts_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::StartWith(_), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like 'a%' or >'a' 不能融合
                    }
                    (LikePattern::StartWith(_), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like 'a%' or >='a' 不能融合
                    }
                    (LikePattern::StartWith(_), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like 'a%' or <'a' 不能融合
                    }
                    (LikePattern::StartWith(_), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like 'a%' or <='a' 不能融合
                    }
                    //-------------------------------------------------------------------------------
                    (LikePattern::EndWith(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like '%bd' or ='abd'
                        if targetString.ends_with(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::EndWith(_), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like '%a' or >'a' 不能融合
                    }
                    (LikePattern::EndWith(_), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like '%a' or >='a' 不能融合
                    }
                    (LikePattern::EndWith(_), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like '%a' or <'a' 不能融合
                    }
                    (LikePattern::EndWith(_), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like '%a' or <='a' 不能融合
                    }
                    //-------------------------------------------------------------------------------
                    (LikePattern::Contain(string), Op::MathCmpOp(MathCmpOp::Equal)) => {
                        // like '%a%' or ='dad'
                        if targetString.contains(string) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), value));
                        }
                    }
                    (LikePattern::Contain(_), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
                        // like '%a%' or >'dad' 不能融合
                    }
                    (LikePattern::Contain(_), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
                        // like '%a%' or >='dad' 不能融合
                    }
                    (LikePattern::Contain(_), Op::MathCmpOp(MathCmpOp::LessThan)) => {
                        // like '%a%' or <'dad' 不能融合
                    }
                    (LikePattern::Contain(_), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
                        // like '%a%' or <='dad' 不能融合
                    }
                    _ => panic!("impossible")
                }
            }
        }
        (_, Op::SqlOp(SqlOp::Like)) => {
            if let GraphValue::String(targetString) = targetValue {
                if let LikePattern::Nonsense = op::determineLikePattern(targetString)? {
                    return Ok(MergeResult::Nonsense);
                }
            }

            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let targetLikePattern = op::determineLikePattern(targetString)?;

                // op 5类, targetLikePattern 4类,这边的左右顺序和上边的是镜像的
                match (op, &targetLikePattern) {
                    //  ='d' or like'd'
                    (Op::MathCmpOp(MathCmpOp::Equal), LikePattern::Equal(targetString)) => {
                        if string == targetString { // 不能使用like那边的targetValue
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::Equal), value));
                        }
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterThan) | Op::MathCmpOp(MathCmpOp::GreaterEqual), LikePattern::Equal(targetString)) => {
                        //  >'d' or like 'd'  , >='d' or like 'd'
                        if string == targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
                        }
                    }
                    (Op::MathCmpOp(MathCmpOp::LessThan) | Op::MathCmpOp(MathCmpOp::LessEqual), LikePattern::Equal(targetString)) => {
                        // <'d' or like 'd' , <='d' or like 'd'
                        if string == targetString {
                            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
                        }
                    }
                    // -------------------------------------------------------------------------
                    (Op::MathCmpOp(MathCmpOp::Equal), LikePattern::StartWith(targetString)) => {
                        //  ='abcd' or like 'a%'
                        if string.starts_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterThan), LikePattern::StartWith(_)) => {
                        // >'a' or like 'a%'  不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterEqual), LikePattern::StartWith(_)) => {
                        // >='a' or like 'a%' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessThan), LikePattern::StartWith(_)) => {
                        // <'a' or like 'a%' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessEqual), LikePattern::StartWith(_)) => {
                        // <='a' or like 'a%' 不能融合
                    }
                    // -------------------------------------------------------------------------
                    (Op::MathCmpOp(MathCmpOp::Equal), LikePattern::EndWith(targetString)) => {
                        //  ='adac' or like '%dac'
                        if string.ends_with(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterThan), LikePattern::EndWith(_)) => {
                        // >'a' or like '%a' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterEqual), LikePattern::EndWith(_)) => {
                        // >='a' or like '%a' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessThan), LikePattern::EndWith(_)) => {
                        // <'a' or like '%a' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessEqual), LikePattern::EndWith(_)) => {
                        // <='a' or like '%a' 不能融合
                    }
                    // --------------------------------------------------------------------------
                    (Op::MathCmpOp(MathCmpOp::Equal), LikePattern::Contain(targetString)) => {
                        // ='dad' or like '%a%'
                        if string.contains(targetString) {
                            return ok_merged!((Op::SqlOp(SqlOp::Like), targetValue));
                        }
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterThan), LikePattern::Contain(_)) => {
                        // >'dad' or like '%a%' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::GreaterEqual), LikePattern::Contain(_)) => {
                        // >='dad' or like '%a%'  不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessThan), LikePattern::Contain(_)) => {
                        // <'dad' or like '%a%' 不能融合
                    }
                    (Op::MathCmpOp(MathCmpOp::LessEqual), LikePattern::Contain(_)) => {
                        // <='dad' or like '%a%' 不能融合
                    }
                    _ => panic!("impossible")
                }
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // 能够融合
                return ok_merged!((Op::MathCmpOp(MathCmpOp::Equal), value));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // =6 or >=6
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
            }
            // =6 or >=7 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value == targetValue { // =6 or >6
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
            }

            if value > targetValue { // =6 or >5
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
            }
            // =6 or >7 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // =6 or <=6, =6 or <=9
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
            }
            // =6 or <=0 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value < targetValue { // =6 or <7
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
            }

            if value == targetValue { // =6 or <6
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
            }
            //  =6 or <0 不能融合
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // >6 or =6
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
            }

            if value <= targetValue {  // >6 or =9
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
            }
            // >=6 or =3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // >6 or >6 , >6 or >3
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
            }

            // >6 or >7
            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >6 and >=6, >6 and >=3
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
            }

            // >3 and >=4
            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >6 or <=6 , >6 or <=7 是废话
                return Ok(MergeResult::Nonsense);
            }
            // >6 or <=3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >3 or <3 等效not equal, >3 or <4 是废话
                return Ok(MergeResult::Nonsense);
            }
            // >3 or <0 不能融合
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value <= targetValue { // >=6 or =6, >=6 or =9
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), value));
            }
            // >=6 or =3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >=6 or >=6 , >=6 or >=0
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue));
            }

            // >=6 or >=7
            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value <= targetValue { // >=6 or > 6, >=6 or >7
                return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterEqual), value));
            }

            // >=6 or >0
            return ok_merged!((Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >=6 or <=6, >=6 or <=9 废话
                return Ok(MergeResult::Nonsense);
            }
            // >=6 or <=0 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >=6 or <6, >=6 or <7 废话
                return Ok(MergeResult::Nonsense);
            }
            // >=6 or <5 不能融合
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value >= targetValue { // <=6 or =6 , <=6 or =5
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
            }
            // <=6 or =9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <=6 and >6, <=6 or >5 废话
                return Ok(MergeResult::Nonsense);
            }
            // <=6 and >9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <=6 or >=6 ,<=6 or >=0 废话
                return Ok(MergeResult::Nonsense);
            }
            // <=6 and >=9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <=6 or <=6 ,<=6 or <=7
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
            }

            // <=6 or <=0
            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value >= targetValue { // <=6 or <6 ,<=6 or <0
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
            }

            // <=6 or <9
            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // <6 or =6
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), value));
            }

            if value > targetValue {  // <6 or =0
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), value));
            }

            // <6 or =9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <6 or >6 等效not equal, <6 or >3 废话
                return Ok(MergeResult::Nonsense);
            }
            // <6 or >9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <6 or >=6, <6 or >=5 废话
                return Ok(MergeResult::Nonsense);
            }
            // <6 or >=9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <6 or <=6, <6 or <=7
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessEqual), targetValue));
            }

            // <6 or <=5
            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), value));
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // <6 or <6 , <6 or <7
                return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), targetValue));
            }

            // <6 or <5
            return ok_merged!((Op::MathCmpOp(MathCmpOp::LessThan), value));
        }
        _ => panic!("impossible")
    }

    // or的兜底是不能融合
    ok_not_merged!((op, value), (targetOp, targetValue))
}