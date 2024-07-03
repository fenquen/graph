use std::ops::Deref;
use crate::graph_value::GraphValue;
use crate::parser::op;
use crate::parser::op::{LikePattern, Op, SqlOp};
use anyhow::Result;

pub(in crate::executor) mod or;
pub(in crate::executor) mod and;

#[derive(Clone, Copy)]
enum Logical {
    Or,
    And,
}

pub(in crate::executor) enum AccumulateResult<'a> {
    Conflict,
    Nonsense,
    Ok(Vec<(Op, &'a GraphValue)>),
}

pub(in crate::executor) enum MergeResult<'a> {
    Conflict,
    Nonsense,
    NotMerged(Vec<(Op, &'a GraphValue)>),
    Merged((Op, &'a GraphValue)),
}

pub(in crate::executor) fn accumulateOr<T: Deref<Target=GraphValue>>(opValueVec: &[(Op, T)]) -> anyhow::Result<AccumulateResult> {
    accumulate(opValueVec, Logical::Or)
}

pub(in crate::executor) fn accumulateAnd<T: Deref<Target=GraphValue>>(opValueVec: &[(Op, T)]) -> anyhow::Result<AccumulateResult> {
    accumulate(opValueVec, Logical::And)
}

fn accumulate<'a, T: Deref<Target=GraphValue>>(opValueVec: &'a [(Op, T)], logical: Logical) -> anyhow::Result<AccumulateResult<'a>> {
    let mut selfAccumulated =
        opValueVec.iter().map(|(op, value)| (*op, &**value)).collect::<Vec<(Op, &'a GraphValue)>>();

    // 要是这个闭包的那个&GraphValue 不去标生命周期参数的话会报错,
    // 原因是 编译器认为那个reference 它跑到了外边的Vec<(Op, &graphValue)> 了 产生了dangling
    // 然而事实上不是这样的 然而编译器是不知道的 需要手动的标上
    let accumulate = |op: Op, value: &'a GraphValue, dest: &mut Vec<(Op, &'a GraphValue)>| {
        let mut merged = false;

        // 要是累加的成果还是空的话,直接的insert
        if dest.is_empty() {
            // 单个的opValue本身也是需要检查的 目前已知要应对Nonsense,like '%%' 这样的废话
            if let (Op::SqlOp(SqlOp::Like), GraphValue::String(s)) = (op, value) {
                if let LikePattern::Nonsense = op::determineLikePattern(s)? {
                    return anyhow::Result::<(AccumulateResult<'a>, bool)>::Ok((AccumulateResult::Nonsense, merged));
                }
            }

            dest.push((op, value));
            return anyhow::Result::<(AccumulateResult<'a>, bool)>::Ok((AccumulateResult::Ok(vec![]), merged));
        }

        let mut accumulated = Vec::new();

        for (previousOp, previousValue) in &*dest {
            if merged {
                accumulated.push((*previousOp, *previousValue));
                continue;
            }

            let withSingle = match logical {
                Logical::Or => or::opValueOrOpValue(op, value, *previousOp, previousValue)?,
                Logical::And => and::opValueAndOpValue(op, value, *previousOp, previousValue)?
            };

            match withSingle {
                MergeResult::Nonsense => { // and 和 or 的时候都有可能 ,and的可能情况是 like '%%'
                    if let Logical::Or = logical {
                        // 说明有 a<0 or a>=0 类似的废话出现了
                        return anyhow::Result::<(AccumulateResult<'a>, bool)>::Ok((AccumulateResult::Nonsense, merged));
                    }

                    // 到了这里说明是and,and的时候碰到 like '%%' 相当空气, 其实算merged
                    merged = true;
                    continue; // 未往accumulated里边放东西
                }
                MergeResult::Conflict => {
                    // 只可能是and
                    //assert_eq!(logical, Logical::And);

                    // 说明有 a<0 and a>0 这样的矛盾显现了
                    return Result::<(AccumulateResult<'a>, bool)>::Ok((AccumulateResult::Conflict, merged));
                }
                MergeResult::NotMerged(_) => {
                    accumulated.push((*previousOp, previousValue));
                }
                MergeResult::Merged(mergedOpValue) => {
                    merged = true;
                    accumulated.push(mergedOpValue);
                }
            }
        }

        if merged == false {
            accumulated.push((op, value));
        }

        dest.clear();
        for a in accumulated {
            dest.push(a);
        }
        // selfAccumulated = accumulated;

        return anyhow::Result::<(AccumulateResult<'a>, bool)>::Ok((AccumulateResult::Ok(vec![]), merged));
    };

    loop {
        let clone = selfAccumulated.clone();
        selfAccumulated.clear();

        let mut a = false;

        for (op, value) in clone {
            if let (accumulateResult, merged) = accumulate(op, value, &mut selfAccumulated)? {
                match accumulateResult {
                    AccumulateResult::Conflict => {
                        //assert_eq!(logical, Logical::And);

                        return Ok(AccumulateResult::Conflict);
                    }
                    AccumulateResult::Nonsense => {
                        match logical {
                            Logical::Or => return Ok(AccumulateResult::Nonsense),
                            Logical::And => continue,
                        }
                    }
                    _ => {
                        if merged {
                            a = true;
                        }
                    }
                }
            }
        }

        // 说明未发生过融合,没有进1步融合压缩的可能了
        if a == false {
            break;
        }
    }

    // 没有筛选的条件了 不管是or还是and都意味着是Nonsense
    if selfAccumulated.is_empty() {
        return Ok(AccumulateResult::Nonsense);
    }

    Ok(AccumulateResult::Ok(selfAccumulated))
}

#[macro_export]
macro_rules! ok_some_vec {
    ($($a:tt)*) => {
        Ok(Some(vec![$($a)*]))
    };
}

#[macro_export]
macro_rules! ok_merged {
    ($opValue:expr) => {
        Ok(MergeResult::Merged($opValue))
    };
}

#[macro_export]
macro_rules! ok_not_merged {
    ($($opValue:tt)*) => {
        Ok(MergeResult::NotMerged(vec![$($opValue)*]))
    };
}