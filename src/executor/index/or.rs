use crate::graph_value::GraphValue;
use crate::parser::op::{LikePattern, MathCmpOp, Op, SqlOp};
use crate::{global, utils};
use crate::parser::op;
use anyhow::Result;

/// 如果能融合的话 得到的vec的len是1 不然是2
/// 融合是相当有必要的 不然后续index搜索的时候会有很多无谓的重复 对性能有损失的
pub(super) fn orWithSingle<'a>(op: Op, value: &'a GraphValue,
                               targetOp: Op, targetValue: &'a GraphValue) -> Result<Option<Vec<(Op, &'a GraphValue)>>> {
    assert!(op.permitByIndex());
    assert!(value.isConstant());

    assert!(targetOp.permitByIndex());
    assert!(targetValue.isConstant());

    // 能保证调用本函数的时候,in 已经被消化掉了变成了多个equal
    match (op, targetOp) {
        // 不像其它的,对like来说还要深入其数据种类来探讨,不过不这样的话 like '%' 这样的废话就漏过了
        // like '%a' or >='a'
        (Op::SqlOp(SqlOp::Like), Op::SqlOp(SqlOp::Like)) => {
            // 不在乎具体的数据种类 ,不管是 like null 和 like null 还是 like '%a' 和 like '%a'
            if value == targetValue {
                return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
            }

            // like '%' 含有Redundant踢掉
            if let (GraphValue::String(string)) = value {
                if let LikePattern::Redundant = op::determineLikePattern(string)? {
                    return Ok(None);
                }
            }

            // like '%' 含有Redundant踢掉
            if let (GraphValue::String(targetString)) = targetValue {
                if let LikePattern::Redundant = op::determineLikePattern(targetString)? {
                    return Ok(None);
                }
            }
            
            if let (GraphValue::String(string), GraphValue::String(targetString)) = (value, targetValue) {
                let likePattern = op::determineLikePattern(string)?;
                let targetLikePattern = op::determineLikePattern(targetString)?;

                // 含有Redundant踢掉,事实上是用不着的上边的会兜底的,写了是为了后续阅读方便
                if let LikePattern::Redundant = &likePattern {
                    return Ok(None);
                }
                if let LikePattern::Redundant = &targetLikePattern {
                    return Ok(None);
                }

                let likeString = likePattern.getString()?;
                let targetLikeString = targetLikePattern.getString()?;

                match (&likePattern, &targetLikePattern) { // 统共要有16类情况
                    (LikePattern::Equal(_), _) | (_, LikePattern::Equal(_)) => { // 对应7类情况 4+4-1
                        // like 'a' or like 'a'
                        // like 'a' or like 'a%'
                        // like 'a' or like '%a'
                        // like 'a' or like '%a%'
                        if likeString == targetLikeString {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }
                    (LikePattern::StartWith(string), LikePattern::StartWith(targetString)) => {
                        // like 'a%' or like 'abcd%'
                        if targetString.starts_with(string) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                        }

                        // like 'abcd%' or like 'a%'
                        if string.starts_with(targetString) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }
                    (LikePattern::StartWith(string), LikePattern::EndWith(targetString)) => {
                        // like 'a%' or like '%a' ,不能融合
                    }
                    (LikePattern::StartWith(string), LikePattern::Contain(targetString)) => {
                        // like 'abcd%' or like '%b%'
                        if string.contains(targetString) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }
                    (LikePattern::EndWith(string), LikePattern::StartWith(targetString)) => {
                        // like '%a' or like 'a%' ,不能融合
                    }
                    (LikePattern::EndWith(string), LikePattern::EndWith(targetString)) => {
                        // like '%d' or like '%abcd'
                        if targetString.ends_with(string) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                        }

                        // like '%abcd' or like '%d'
                        if string.ends_with(targetString) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }
                    (LikePattern::EndWith(string), LikePattern::Contain(targetString)) => {
                        // like '%abcd' or like '%b%'
                        if string.contains(targetString) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::StartWith(targetString)) => {
                        // like '%a%' or like 'bacd%'
                        if targetString.contains(string) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::EndWith(targetString)) => {
                        // like '%a%' or like '%bacd'
                        if targetString.contains(string) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                        }
                    }
                    (LikePattern::Contain(string), LikePattern::Contain(targetString)) => {
                        // like '%abd%' or like '%dabdr%'
                        if targetString.contains(string) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                        }

                        // like '%dabdr' or like '%abd%'
                        if string.contains(targetString) {
                            return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), targetValue)]));
                        }
                    }

                    _ => panic!("impossible")
                }
            }
        }
        (Op::SqlOp(SqlOp::Like), _) => {
            assert_eq!(value.getType(), GraphValue::STRING);
            assert_eq!(targetValue.getType(), GraphValue::STRING);

            let selfString = value.asString()?;
            let targetString = value.asString()?;

            match targetOp {
                Op::MathCmpOp(MathCmpOp::Equal) => { // like '%a' or ='a'
                    // like 'b' or ='b'
                }
                Op::MathCmpOp(MathCmpOp::GreaterEqual) => {
                    // like 'a%' or >='a'
                }
                Op::MathCmpOp(MathCmpOp::GreaterThan) => {}
                Op::MathCmpOp(MathCmpOp::LessEqual) => {}
                Op::MathCmpOp(MathCmpOp::LessThan) => {}
                Op::SqlOp(SqlOp::Like) => { // 不在乎具体的数据种类
                    if value == targetValue {
                        return Ok(Some(vec![(Op::SqlOp(SqlOp::Like), value)]));
                    }
                }
                _ => panic!()
            }
        }
        (_, Op::SqlOp(SqlOp::Like)) => {}
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // 能够融合
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::Equal), value)]));
            }
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // =6 or >=6
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)]));
            }
            // =6 or >=7 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value == targetValue { // =6 or >6
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)]));
            }

            if value > targetValue { // =6 or >5
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)]));
            }
            // =6 or >7 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // =6 or <=6, =6 or <=9
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)]));
            }
            // =6 or <=0 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::Equal), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value < targetValue { // =6 or <7
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)]));
            }

            if value == targetValue { // =6 or <6
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)]));
            }
            //  =6 or <0 不能融合
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // >6 or =6
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)]));
            }

            if value <= targetValue {  // >6 or =9
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)]));
            }
            // >=6 or =3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // >6 or >6 , >6 or >3
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)]));
            }

            // >6 or >7
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)]));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >6 and >=6, >6 and >=3
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)]));
            }

            // >3 and >=4
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)]));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >6 or <=6 , >6 or <=7 是废话
                return Ok(None);
            }
            // >6 or <=3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >3 or <3 等效not equal, >3 or <4 是废话
                return Ok(None);
            }
            // >3 or <0 不能融合
        }
        // -----------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value <= targetValue { // >=6 or =6, >=6 or =9
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), value)]));
            }
            // >=6 or =3 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // >=6 or >=6 , >=6 or >=0
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), targetValue)]));
            }

            // >=6 or >=7
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)]));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value <= targetValue { // >=6 or > 6, >=6 or >7
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterEqual), value)]));
            }

            // >=6 or >0
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::GreaterThan), targetValue)]));
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // >=6 or <=6, >=6 or <=9 废话
                return Ok(None);
            }
            // >=6 or <=0 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::GreaterEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // >=6 or <6, >=6 or <7 废话
                return Ok(None);
            }
            // >=6 or <5 不能融合
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value >= targetValue { // <=6 or =6 , <=6 or =5
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)]));
            }
            // <=6 or =9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <=6 and >6, <=6 or >5 废话
                return Ok(None);
            }
            // <=6 and >9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <=6 or >=6 ,<=6 or >=0 废话
                return Ok(None);
            }
            // <=6 and >=9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <=6 or <=6 ,<=6 or <=7
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)]));
            }

            // <=6 or <=0
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)]));
        }
        (Op::MathCmpOp(MathCmpOp::LessEqual), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value >= targetValue { // <=6 or <6 ,<=6 or <0
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)]));
            }

            // <=6 or <9
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)]));
        }
        // ------------------------------------------------------------------------------
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::Equal)) => {
            if value == targetValue { // <6 or =6
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), value)]));
            }

            if value > targetValue {  // <6 or =0
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)]));
            }

            // <6 or =9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterThan)) => {
            if value >= targetValue { // <6 or >6 等效not equal, <6 or >3 废话
                return Ok(None);
            }
            // <6 or >9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::GreaterEqual)) => {
            if value >= targetValue { // <6 or >=6, <6 or >=5 废话
                return Ok(None);
            }
            // <6 or >=9 不能融合
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessEqual)) => {
            if value <= targetValue { // <6 or <=6, <6 or <=7
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessEqual), targetValue)]));
            }

            // <6 or <=5
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)]));
        }
        (Op::MathCmpOp(MathCmpOp::LessThan), Op::MathCmpOp(MathCmpOp::LessThan)) => {
            if value <= targetValue { // <6 or <6 , <6 or <7
                return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), targetValue)]));
            }

            // <6 or <5
            return Ok(Some(vec![(Op::MathCmpOp(MathCmpOp::LessThan), value)]));
        }
        _ => panic!("impossible")
    }

    return Ok(Some(vec![(op, value), (targetOp, targetValue)]));
}