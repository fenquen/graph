use std::collections::HashMap;
use std::rc::Rc;
use crate::executor::optimizer;
use crate::executor::optimizer::merge::AccumulateResult;
use crate::executor::store::ScanParams;
use crate::graph_value::GraphValue;
use crate::parser::op::{MathCmpOp, Op};
use crate::suffix_plus_plus;
use anyhow::Result;
use crate::executor::optimizer::merge;
use crate::expr::Expr;

#[derive(Default)]
struct AndDesc<'a> {
    parent: Option<Rc<AndDesc<'a>>>,
    op: Option<Op>,
    value: Option<&'a GraphValue>,
}

/// 提前感知到 and 的时候的 conflict 和 nonsense
pub(in crate::executor) fn processTableFilter(tableFilter: &Expr) -> Result<TableFilterProcResult> {
    // 要把tableFilter上涉及到的columnName的expr全部提取
    // tableFilter上的字段名->Vec<(op, value)>
    let mut indexableTableFilterColName_opValuesVec = HashMap::default();

    // 单个字段上的各个opValue之间 以及 各column之间 是and还是or, 目前感觉实现的还是不够精细
    // 如果是and的话是真的纯and, 如果是or的话 不1定是纯or
    let mut isPureAnd = true;
    let mut isPureOr = true;
    let mut hasExprAbandonedByIndex = false;
    let mut columnNameExist = false;

    tableFilter.collectColNameValue(&mut indexableTableFilterColName_opValuesVec, &mut isPureAnd, &mut isPureOr, &mut hasExprAbandonedByIndex, &mut columnNameExist)?;

    // 包含pureOr和不是纯or
    let mut orHasNonsense = false;

    // 说明tableFilter上未写column名,那么tableFilter是可以直接计算的
    if columnNameExist == false {
        return Ok(TableFilterProcResult::NoColumnNameInTableFilter);
    }

    let tableFilterColCount = indexableTableFilterColName_opValuesVec.len();

    let mut tableFilterColName_opValueVecVec = HashMap::with_capacity(tableFilterColCount);

    // and不光有压缩 还有风险的提前识别
    if isPureAnd {
        let mut nonsenseColCount = 0usize;

        // 不管如何 都先将opValue先压缩
        'loopTableFilterColumnName:
        for (tableFilterColumnName, opValuesVec) in &indexableTableFilterColName_opValuesVec {
            // 收集了全部的leaf node到时候遍历溯源
            let mut leafVec = Vec::new();

            // opValueVecVec下的各个opValueVec之间是or, opValueVec下的各个opValue是and
            let mut opValueVecVec = Vec::new();

            and(opValuesVec, Rc::new(AndDesc::default()), &mut leafVec);

            // 对各个的leaf遍历
            for leaf in &leafVec {
                let mut opValueVec = Vec::new();

                let mut current = leaf;

                if let (Some(op), Some(value)) = (current.op, &current.value) {
                    opValueVec.push((op, *value))
                }

                // 不断的向上
                while let Some(parent) = current.parent.as_ref() {
                    if let (Some(op), Some(value)) = (parent.op, &parent.value) {
                        opValueVec.push((op, *value))
                    }

                    current = parent;
                }

                // 各个的opValueVec 它们之间是or的,opValueVec内部的各个元素是and的
                opValueVecVec.push(opValueVec);
            }

            //  对当前这个的column麾下的各个的and脉络压缩
            let opValueVecVec = {
                let mut a: Vec<Vec<(Op, &GraphValue)>> = Vec::with_capacity(opValueVecVec.len());

                let mut confilctCount = 0usize;

                for opValueVec in &opValueVecVec {
                    match merge::accumulateAnd(opValueVec.as_slice())? {
                        // a>=0 and a<0 矛盾
                        // 要是全部的脉络都是Conflict的话 那不止是用不用index的问题了 select是没有相应的必要的
                        AccumulateResult::Conflict => {
                            suffix_plus_plus!(confilctCount);
                            continue;
                        }
                        // 只要有1条脉络是Nonsense,那么对该col的筛选等于废话成为透明
                        AccumulateResult::Nonsense => {
                            suffix_plus_plus!(nonsenseColCount);
                            continue 'loopTableFilterColumnName; // 换个tableFilterColumn 对pureAnd来说 抠掉也是可以的
                        }
                        AccumulateResult::Ok(opValueVec) => a.push(opValueVec)
                    }
                }

                // 这个column上的筛选条件是不成立的
                if confilctCount == opValueVecVec.capacity() {
                    assert!(opValuesVec.is_empty());
                    return Ok(TableFilterProcResult::IndexableTableFilterColHasConflictWhenIsPureAnd);
                }

                a
            };

            // 如果到这边打算收场的话 莫忘了将&GraphValue变为GraphValue
            let opValueVecVec: Vec<Vec<(Op, GraphValue)>> =
                opValueVecVec.iter().map(|opValueVec| {
                    opValueVec.iter().map(|(op, value)| {
                        (*op, (*value).clone())
                    }).collect::<Vec<(Op, GraphValue)>>()
                }).collect::<>();

            // 尝试or压缩 (a and b) or (c and d), 应对 (a and b)和(c and d) 之间重复的部分
            // 如果是纯粹通用考虑的话是不太容易的, 不过以目前的话事实上是可以知道的, 如果

            tableFilterColName_opValueVecVec.insert(tableFilterColumnName.clone(), opValueVecVec);
        }

        // and情况下 tableFilter上的各个col的筛选全都是nonsense
        if nonsenseColCount == tableFilterColCount {
            assert!(tableFilterColName_opValueVecVec.is_empty());
            return Ok(TableFilterProcResult::AllIndexableTableFilterColsAreNonsenseWhenIsPureAnd { hasExprAbandonedByIndex });
        }
    } else {
        'tableFilterColumnName:
        for (tableFilterColumnName, opValuesVec) in &indexableTableFilterColName_opValuesVec {
            // 扁平化opValuesVec 变为 opValueVec
            let opValueVec = {
                let mut opValueVec = Vec::new();

                for (op, values) in opValuesVec {
                    assert!(op.permitByIndex());

                    // 说明是尚未被消化的in
                    if values.len() > 1 {
                        for value in values {
                            assert!(value.isConstant());

                            opValueVec.push((Op::MathCmpOp(MathCmpOp::Equal), value));
                        }
                    } else {
                        let value = values.first().unwrap();
                        assert!(value.isConstant());

                        opValueVec.push((*op, value));
                    }
                }
                opValueVec
            };

            let opValueVec =
                match merge::accumulateOr(opValueVec.as_slice())? {
                    AccumulateResult::Conflict => panic!("impossible"),
                    // a>0 or a<=0 这样的废话
                    AccumulateResult::Nonsense => {
                        if isPureOr {
                            return Ok(TableFilterProcResult::IndexableTableFilterColHasNonesenseWhenIsPureOr);
                        }

                        // continue 'tableFilterColumnName;

                        // 不能跳过因为不是pureOr, 还是占位1个空的 opValueVec ,没有限制条件也就意味着必然是true 也能体现nonsense意思
                        // 这个不光是要为index考虑 还要有别的考量
                        vec![]
                    }
                    AccumulateResult::Ok(opValueVec) => opValueVec,
                };

            let opValueVec: Vec<(Op, GraphValue)> = opValueVec.into_iter().map(|(op, value)| { (op, value.clone()) }).collect();

            tableFilterColName_opValueVecVec.insert(tableFilterColumnName.clone(), vec![opValueVec]);
        }
    }

    return Ok(
        TableFilterProcResult::MaybeCanUseIndex {
            indexableTableFilterColName_opValueVecVec: tableFilterColName_opValueVecVec,
            isPureAnd,
            isPureOr,
            orHasNonsense,
        }
    );
}

// 生成向上溯源的树 因为它只有parent
fn and<'a>(opValuesVec: &'a [(Op, Vec<GraphValue>)],
           parent: Rc<AndDesc<'a>>,
           leafVec: &mut Vec<AndDesc<'a>>) {
    for (op, values) in opValuesVec {
        if values.len() > 1 {
            // assert_eq!(*op, Op::SqlOp(SqlOp::In));

            // 如果in出现在了 and 体系 那么 各个单独的脉络是and 且result必然是equal 脉络之间是and
            for value in values {
                let mut andDesc = AndDesc::default();
                andDesc.parent = Some(parent.clone());
                andDesc.op = Some(Op::MathCmpOp(MathCmpOp::Equal));
                andDesc.value = Some(value);

                // 不是last元素
                if opValuesVec.len() > 1 {
                    // 收纳小弟
                    and(&opValuesVec[1..], Rc::new(andDesc), leafVec);
                } else {
                    leafVec.push(andDesc);
                }
            }

            // 注意需要return刹住
            return;
        }

        let value = values.first().unwrap();
        assert!(value.isConstant());

        let mut andDesc = AndDesc::default();
        andDesc.parent = Some(parent.clone());
        andDesc.op = Some(*op);
        andDesc.value = Some(value);

        // 不是last元素
        if opValuesVec.len() > 1 {
            // 收纳小弟,注意需要return刹住
            return and(&opValuesVec[1..], Rc::new(andDesc), leafVec);
        }

        leafVec.push(andDesc);
    }
}

pub(in crate::executor) enum TableFilterProcResult {
    AllIndexableTableFilterColsAreNonsenseWhenIsPureAnd { hasExprAbandonedByIndex: bool },
    IndexableTableFilterColHasConflictWhenIsPureAnd,
    IndexableTableFilterColHasNonesenseWhenIsPureOr,
    NoColumnNameInTableFilter,
    MaybeCanUseIndex {
        indexableTableFilterColName_opValueVecVec: HashMap<String, Vec<Vec<(Op, GraphValue)>>>,
        isPureAnd: bool,
        isPureOr: bool,
        /// 意味着至少是部分or(tableFilter含有or)且碰到了 a <=0 or a>0 这样的废话
        orHasNonsense: bool,
    },
}