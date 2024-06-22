pub(super) mod or;
pub(super) mod and;

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::rc::Rc;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::one::Ref;
use serde_json::Value;
use crate::graph_value::GraphValue;
use crate::parser::op::{MathCmpOp, Op};
use crate::executor::{CommandExecutor, index};
use crate::expr::Expr;
use crate::meta::{DBObject, Table};
use crate::{meta, suffix_plus_plus, throwFormat, byte_slice_to_u64, global};
use crate::codec::{BinaryCodec, MyBytes};
use crate::executor::store;
use crate::session::Session;
use crate::types::{Byte, DataKey, DBRawIterator, RowData};
use anyhow::Result;

#[derive(Clone, Copy)]
pub enum Logical {
    Or,
    And,
}

pub(super) fn accumulateOr<T: Deref<Target=GraphValue>>(opValueVec: &[(Op, T)]) -> Option<Vec<(Op, &GraphValue)>> {
    accumulate(opValueVec, Logical::Or)
}

pub(super) fn accumulateAnd<T: Deref<Target=GraphValue>>(opValueVec: &[(Op, T)]) -> Option<Vec<(Op, &GraphValue)>> {
    accumulate(opValueVec, Logical::And)
}

fn accumulate<'a, T: Deref<Target=GraphValue>>(opValueVec: &'a [(Op, T)], logical: Logical) -> Option<Vec<(Op, &'a GraphValue)>> {
    let mut selfAccumulated = Vec::new();

    // 要是这个闭包的那个&GraphValue 不去标生命周期参数的话会报错,
    // 原因是 编译器认为那个reference 它跑到了外边的Vec<(Op, &graphValue)> 了 产生了dangling
    // 然而事实上不是这样的 然而编译器是不知道的 需要手动的标上
    let mut accumulate = |op: Op, value: &'a GraphValue| {
        let mut merged = false;

        // 第1趟
        if selfAccumulated.is_empty() {
            selfAccumulated.push((op, value));
            return true;
        }

        let mut accumulated = Vec::with_capacity(selfAccumulated.len());

        for (previousOp, previousValue) in &selfAccumulated {
            if merged {
                accumulated.push((*previousOp, *previousValue));
                continue;
            }

            let withSingle = match logical {
                Logical::Or => or::orWithSingle(op, value, *previousOp, previousValue),
                Logical::And => and::andWithSingle(op, value, *previousOp, previousValue)
            };

            match withSingle {
                None => return false, // 说明有 a<0 or a>=0 类似的废话出现了
                Some(orResult) => {
                    if orResult.len() == 1 { // 说明能融合
                        accumulated.push(orResult[0]);
                        merged = true;
                    } else {
                        accumulated.push((*previousOp, *previousValue));
                    }
                }
            }
        }

        if merged == false {
            accumulated.push((op, value));
        }

        selfAccumulated = accumulated;

        true
    };

    for (op, value) in opValueVec {
        if accumulate(*op, &**value) == false {
            return None;
        }
    }

    Some(selfAccumulated)
}

#[macro_export]
macro_rules! extractDataKeyFromIndexKey {
    ($indexKey: expr) => {
        {
            let dataKey = &$indexKey[($indexKey.len() - meta::DATA_KEY_BYTE_LEN)..];
            byte_slice_to_u64!(dataKey) as crate::types::DataKey
        }
    };
}

pub(in crate::executor) struct IndexSearch<'a> {
    pub(in crate::executor) dbObjectIndex: Ref<'a, String, DBObject>,
    /// 包含的grapgValue 只可能是 IndexUseful
    pub(in crate::executor) opValueVecVecAcrossIndexFilteredCols: Vec<Vec<Vec<(Op, GraphValue)>>>,
    /// 如果说index能够 包含filter的全部字段 和 包含select的全部字段,那么就不用到原表上再搜索了,能够直接就地应对
    pub(in crate::executor) indexLocalSearch: bool,
    pub(in crate::executor) selectedColNames: Option<&'a [String]>,
    pub(in crate::executor) isAnd: bool,
}

impl<'session> CommandExecutor<'session> {
    // todo table对应的index列表 是不是应该融入到table对象 完成
    pub(super) fn getMostSuitableIndex<'a>(&'a self, // 对self使用 'a的原因是 dbObjectIndex是通过 self.getDBObjectByName() 得到 含有的生命周期是 'session
                                           table: &Table, tableFilter: &Expr,
                                           selectedColNames: Option<&'a [String]>) -> anyhow::Result<Option<IndexSearch<'a>>> {
        if table.indexNames.is_empty() {
            return Ok(None);
        }

        // 要把tableFilter上涉及到的columnName的expr全部提取
        // tableFilter上的字段名->Vec<(op, value)>
        let mut tableFilterColName_opValuesVec = HashMap::default();
        // 单个字段上的各个opValue之间 以及 各column之间 是and还是or, 目前感觉实现的还是不够精细
        let mut isAnd = true;

        // 扫描filter 写入
        tableFilter.collectColNameValue(&mut tableFilterColName_opValuesVec, &mut isAnd)?;

        // 说明tableFilter上未写column名,那么tableFilter是可以直接计算的
        if tableFilterColName_opValuesVec.is_empty() {
            return Ok(None);
        }

        let tableFilterColNames: Vec<&String> = tableFilterColName_opValuesVec.keys().collect();

        // 对or来说对话 要想使用index 先要满足 tableFilter只能有1个字段,然后 该字段得是某个index的打头字段
        // 例如 有个index包含 a和b两个字段 对 a=1 or b=2 来说 是用不了该index的 因为应对不了b=2 它不是index的打头部分
        if isAnd == false {
            // 有多个字段 用不了index
            if tableFilterColNames.len() > 1 {
                return Ok(None);
            }
        }

        // 候选的index名单
        let mut candiateInices = Vec::with_capacity(table.indexNames.len());

        // todo 要是有多个index都能应对tableFilter应该挑选哪个 需要考虑 select和filter的涵盖
        // 挑选index 目前的原则有  index的本身能涵盖多少selectedColName, index能涵盖多少过滤条件
        // top 理想的情况是, index的本身能涵盖全部的selectedColName 且 能涵盖全部过滤条件
        // 要是不能的话 都得要去原始的表上 还是优先 覆盖过滤条件多的
        // 遍历table的各个index,筛掉不合适的
        'loopIndex:
        for indexName in &table.indexNames {
            let dbObjectIndex = self.getDBObjectByName(indexName)?;
            let index = dbObjectIndex.asIndex()?;

            // filter能用到index的几个字段
            // tableFilter的字段和index的字段就算有交集,tableFilter的字段也兜底要包含index的第1个字段
            // 例如 (b=1 and c=3),虽然index含有字段a,b,c,然而tableFilter未包含打头的a字段 不能使用
            // (a=1 and c=3) 虽然包含了打头的a字段,然而也只能用到index的a字段部分 因为缺了b字段 使得c用不了
            let mut indexFilteredColNames = Vec::with_capacity(index.columnNames.len());

            // select 要是指明 colName 的话能用到index上的多少字段
            let mut indexSelectedColCount = 0usize;

            // index的各个用到的column上的表达式的集合
            let mut opValueVecVecAcrossIndexFilteredCols = Vec::with_capacity(index.columnNames.len());

            // 遍历index的各columnName
            for indexColName in &index.columnNames {
                if tableFilterColNames.contains(&indexColName) == false {
                    break;
                }

                if let Some(selectedColNames) = selectedColNames {
                    if selectedColNames.contains(indexColName) {
                        suffix_plus_plus!(indexSelectedColCount);
                    }
                }

                // 应对废话 a>0  a<=0 如果是第1个那么index失效 如果是第2个那么有效范围到1
                // 它是下边的&Graph源头
                let opValuesVec = tableFilterColName_opValuesVec.get(indexColName).unwrap();

                #[derive(Default)]
                struct AndDesc<'a> {
                    parent: Option<Rc<AndDesc<'a>>>,
                    op: Option<Op>,
                    value: Option<&'a GraphValue>,
                }

                // and 体系 单个字段上的过滤条件之间是and 字段和字段之间是and
                if isAnd {
                    // 收集了全部的leaf node到时候遍历溯源
                    let mut leafVec = Vec::new();
                    // opValueVecVec下的各个opValueVec之间是or, opValueVec下的各个opValue是and
                    let mut opValueVecVec = Vec::new();

                    let ancestor = Rc::new(AndDesc::default());

                    and(opValuesVec, ancestor, &mut leafVec);

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

                    //  对麾下的各个的and脉络压缩
                    let opValueVecVec: Vec<Vec<(Op, &GraphValue)>> =
                        opValueVecVec.iter().filter_map(|opValueVec| index::accumulateAnd(opValueVec.as_slice())).collect();

                    if opValueVecVec.is_empty() {
                        continue 'loopIndex;
                    }

                    // 如果到这边打算收场的话 莫忘了将&GraphValue变为GraphValue
                    let opValueVecVec: Vec<Vec<(Op, GraphValue)>> =
                        opValueVecVec.iter().map(|opValueVec| {
                            opValueVec.iter().map(|(op, value)| {
                                (*op, (*value).clone())
                            }).collect::<Vec<(Op, GraphValue)>>()
                        }).collect::<>();

                    opValueVecVecAcrossIndexFilteredCols.push(opValueVecVec);

                    // 尝试or压缩 (a and b) or (c and d), 应对 (a and b)和(c and d) 之间重复的部分
                    // 如果是纯粹通用考虑的话是不太容易的, 不过以目前的话事实上是可以知道的, 如果
                } else { // 单个字段上的过滤条件之间是or 字段和字段之间是or
                    let opValueVec: Vec<(Op, &GraphValue)> = opValuesVec.iter().map(|(op, values)| {
                        assert!(op.permitByIndex());
                        assert_eq!(values.len(), 1);

                        let value = values.first().unwrap();
                        assert!(value.isConstant());

                        (*op, value)
                    }).collect();

                    let accumulatedOr = match index::accumulateOr(opValueVec.as_slice()) {
                        Some(accumulated) => accumulated,
                        // 如果and 那么是 a>=0 and a<0 矛盾
                        // 如果是or 那么是 a>0 or a<=0 这样的废话
                        None => continue 'loopIndex
                    };

                    let accumulatedOr: Vec<(Op, GraphValue)> = accumulatedOr.into_iter().map(|(op, value)| { (op, value.clone()) }).collect();
                    opValueVecVecAcrossIndexFilteredCols.push(vec![accumulatedOr]);

                    indexFilteredColNames.push(indexColName.clone());
                }

                // 生成向上溯源的树 因为它只有parent
                fn and<'a>(opValuesVec: &'a [(Op, Vec<GraphValue>)], parent: Rc<AndDesc<'a>>, leafVec: &mut Vec<AndDesc<'a>>) {
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
                        } else {
                            let value = values.first().unwrap();
                            assert!(value.isConstant());

                            let mut andDesc = AndDesc::default();
                            andDesc.parent = Some(parent.clone());
                            andDesc.op = Some(*op);
                            andDesc.value = Some(value);

                            // 不是last元素
                            if opValuesVec.len() > 1 {
                                // 收纳小弟
                                and(&opValuesVec[1..], Rc::new(andDesc), leafVec);
                            } else {
                                leafVec.push(andDesc);
                            }
                        }
                    }
                }
            }

            // filter没有用到index的任何字段
            if indexFilteredColNames.is_empty() {
                continue;
            }

            // 不能直接放index 因为它是来源dbObject的 而for 循环结束后dbObject销毁了
            candiateInices.push((dbObjectIndex, indexSelectedColCount, indexFilteredColNames, opValueVecVecAcrossIndexFilteredCols));
        }

        if candiateInices.is_empty() {
            return Ok(None);
        }

        // indexFilteredColNames 由大到小排序
        candiateInices.sort_by(|prev, next| {
            // 比较 filter用到的字段数量
            let compareFilterdColCount = next.2.len().cmp(&prev.2.len());

            // 要是相同 然后去比较 select用到的字段数量
            if let cmp::Ordering::Equal = compareFilterdColCount {
                return next.1.cmp(&prev.1);
            }

            compareFilterdColCount
        });

        // 目前的话实现的比较粗糙,排前头的几个要是 indexFilteredColNames 大小相同 选第1个
        let (dbObjectIndex,
            indexSelectedColCount,
            indexFilteredColNames,
            opValueVecVecAcrossIndexFilteredCols) = candiateInices.remove(0);

        //  对拥有相同 indexFilteredColNames 的多个 index 的筛选
        // index字段要覆盖全部的过滤条件

        // value 和 column的type是不是匹配
        for index in 0..indexFilteredColNames.len() {
            let columnNameFromIndexUsed = indexFilteredColNames.get(index).unwrap();
            for column in &table.columns {
                if column.name.as_str() != columnNameFromIndexUsed {
                    continue;
                }

                let opValueVecVec = opValueVecVecAcrossIndexFilteredCols.get(index).unwrap();

                for opValueVec in opValueVecVec {
                    for (_, value) in opValueVec {
                        if column.type0.compatible(value) == false {
                            throwFormat!("table: {}, column:{}, type:{} is not compatible with value:{}",
                                table.name, columnNameFromIndexUsed, column.type0, value)
                        }
                    }
                }
            }
        }

        let indexLocalSearch = {
            let mut indexLocalSearch = false;

            if let Some(selectedColNames) = selectedColNames {
                // 覆盖全部的select 字段
                if indexSelectedColCount == selectedColNames.len() {
                    // 覆盖全部的过滤字段
                    if indexFilteredColNames.len() == tableFilterColNames.len() {
                        indexLocalSearch = true;
                    }
                }
            }

            indexLocalSearch
        };

        let indexSearch = IndexSearch {
            dbObjectIndex,
            opValueVecVecAcrossIndexFilteredCols,
            indexLocalSearch,
            selectedColNames,
            isAnd,
        };

        Ok(Some(indexSearch))
    }

    // todo 如果index本身能包含要select的全部字段 那么直接index读取了
    /// index本身也是1个table 保存的不过是dataKey
    pub(in crate::executor) fn searchByIndex(&self, indexSearch: IndexSearch) -> anyhow::Result<()> {
        let index = indexSearch.dbObjectIndex.asIndex()?;
        let snapshot = self.session.getSnapshot()?;

        let indexColumnFamily = Session::getColFamily(index.name.as_str())?;
        let mut dbRawIterator: DBRawIterator = snapshot.raw_iterator_cf(&indexColumnFamily);

        let tableObject = self.getDBObjectByName(index.tableName.as_str())?;
        let table = tableObject.asTable()?;

        // or的情况要使用index的话, 过滤条件的字段只能是1个 且是 idnex的打头字段
        if indexSearch.isAnd == false {
            assert_eq!(indexSearch.opValueVecVecAcrossIndexFilteredCols.len(), 1);
        }

        // seek那都是要以index的第1个column为切入的, 后边的column是在index数据基础上的筛选
        let opValueVecVecOnIndex1stColumn = indexSearch.opValueVecVecAcrossIndexFilteredCols.first().unwrap();

        let mut dataKeys: HashSet<DataKey> = HashSet::new();

        let mut rowDatas: HashMap<DataKey, Option<RowData>> = HashMap::new();

        let mut lowerValueBuffer = BytesMut::new();
        let mut upperValueBuffer = BytesMut::new();

        let mut buffer = BytesMut::new();

        macro_rules! getKeyIfSome {
            ($dbRawIterator:expr) => {
                {
                    let key = $dbRawIterator.key();
                    if key.is_none() {
                        break;
                    }

                    key.unwrap() as &[Byte]
                }
            };
        }

        // todo 如果是indexLocal的话 还是要应对重复数据 不像应对datakey那样容易
        let mut processWhen1stColSatisfied = |indexKey: &[Byte]| {
            if let Some(indexSearchResult) = self.further(indexKey, &indexSearch)? {
                match indexSearchResult {
                    IndexSearchResult::Direct((dataKey, rowData)) => rowDatas.insert(dataKey, Some(rowData)),
                    IndexSearchResult::Redirect(dataKey) => rowDatas.insert(dataKey, None),
                };
            }

            Result::<()>::Ok(())
        };

        // opValueVecOnIndex1stColumn 之间不管isAnd如何都是 or
        for opValueVecOnIndex1stColumn in opValueVecVecOnIndex1stColumn {
            // opValueVecOnIndex1stColumn 的各个元素(opValueVec)之间是 and 还是 or 取决 isAnd
            if indexSearch.isAnd {
                let mut lowerValue = None;
                let mut lowerInclusive = false;
                let mut upperValue = None;
                let mut upperInclusive = false;

                // opValueVec 上的各个筛选条件之间是and 而且已经压缩过的了
                for (op, value) in opValueVecOnIndex1stColumn {
                    assert!(op.permitByIndex());
                    assert!(value.isConstant());

                    match op {
                        Op::MathCmpOp(MathCmpOp::Equal) => {
                            lowerValue = Some(value);
                            lowerInclusive = true;
                            upperValue = Some(value);
                            upperInclusive = true;
                        }
                        Op::MathCmpOp(MathCmpOp::GreaterThan) => {
                            lowerValue = Some(value);
                            lowerInclusive = false;
                        }
                        Op::MathCmpOp(MathCmpOp::GreaterEqual) => {
                            lowerValue = Some(value);
                            lowerInclusive = true;
                        }
                        Op::MathCmpOp(MathCmpOp::LessEqual) => {
                            upperValue = Some(value);
                            upperInclusive = true;
                        }
                        Op::MathCmpOp(MathCmpOp::LessThan) => {
                            upperValue = Some(value);
                            upperInclusive = false;
                        }
                        _ => panic!("impossible")
                    }
                }

                match (lowerValue, upperValue) {
                    (Some(lowerValue), Some(upperValue)) => {
                        lowerValue.encode(&mut lowerValueBuffer)?;
                        upperValue.encode(&mut upperValueBuffer)?;

                        dbRawIterator.seek(lowerValueBuffer.as_ref());

                        let mut hasBeyondLower = false;

                        loop {
                            let indexKey = getKeyIfSome!(dbRawIterator);

                            // lowerInclusive应对
                            // 使用这个变量的原因是 减少遍历过程中对start_with的调用 要是两边都很大的话成本不小
                            if hasBeyondLower == false {
                                if indexKey.starts_with(lowerValueBuffer.as_ref()) {
                                    if lowerInclusive == false {
                                        dbRawIterator.next();
                                        continue;
                                    }

                                    // 处理
                                    processWhen1stColSatisfied(indexKey)?;

                                    dbRawIterator.next();
                                    continue;
                                } else {
                                    // 应该经历下边的upper上限的check
                                    hasBeyondLower = true;
                                }
                            }

                            // 有没有到了上限了
                            if indexKey.starts_with(upperValueBuffer.as_ref()) {
                                if upperInclusive == false {
                                    break;
                                }
                            } else {
                                break;
                            }

                            // 处理
                            processWhen1stColSatisfied(indexKey)?;

                            dbRawIterator.next();
                        }
                    }
                    (Some(lowerValue), None) => {
                        lowerValue.encode(&mut lowerValueBuffer)?;
                        dbRawIterator.seek(lowerValueBuffer.as_ref());

                        let mut hasBeyondLower = false;

                        loop {
                            let indexKey = getKeyIfSome!(dbRawIterator);

                            if hasBeyondLower == false {
                                if indexKey.starts_with(lowerValueBuffer.as_ref()) {
                                    if lowerInclusive == false {
                                        dbRawIterator.next();
                                        continue;
                                    }
                                } else {
                                    hasBeyondLower = true
                                }
                            }

                            processWhen1stColSatisfied(indexKey)?;

                            dbRawIterator.next()
                        }
                    }
                    (None, Some(upperValue)) => {
                        upperValue.encode(&mut upperValueBuffer)?;
                        dbRawIterator.seek_for_prev(upperValueBuffer.as_ref());

                        let mut startWithUpper = true;

                        loop {
                            let indexKey = getKeyIfSome!(dbRawIterator);

                            if startWithUpper {
                                if indexKey.starts_with(upperValueBuffer.as_ref()) {
                                    if upperInclusive == false {
                                        dbRawIterator.next();
                                        continue;
                                    }
                                } else {
                                    startWithUpper = false;
                                }
                            }

                            processWhen1stColSatisfied(indexKey)?;

                            dbRawIterator.prev();
                        }
                    }
                    (None, None) => panic!("impossible")
                }
            } else {
                // or的时候想要用上index, tableFilter只能有1个字段 ,且是这个index的打头字段
                // opValueVec 上的各个筛选条件之间是 or 而且已经压缩过的了
                for (op, value) in opValueVecOnIndex1stColumn {
                    assert!(op.permitByIndex());
                    assert!(value.isConstant());

                    value.encode(&mut buffer)?;

                    match op {
                        Op::MathCmpOp(MathCmpOp::Equal) => {
                            dbRawIterator.seek(buffer.as_ref());

                            loop {
                                let indexKey = getKeyIfSome!(dbRawIterator);

                                // 说明satisfy
                                if indexKey.starts_with(buffer.as_ref()) == false {
                                    break;
                                }

                                processWhen1stColSatisfied(indexKey)?;

                                dbRawIterator.next();
                            }
                        }
                        Op::MathCmpOp(MathCmpOp::GreaterEqual) => { // 应对 >= 是简单的 1路到底什么inclusive等都不用管
                            dbRawIterator.seek(buffer.as_ref());

                            loop {
                                let indexKey = getKeyIfSome!(dbRawIterator);
                                processWhen1stColSatisfied(indexKey)?;

                                dbRawIterator.next()
                            }
                        }
                        Op::MathCmpOp(MathCmpOp::GreaterThan) => {
                            dbRawIterator.seek(buffer.as_ref());

                            loop {
                                let indexKey = getKeyIfSome!(dbRawIterator);

                                if indexKey.starts_with(buffer.as_ref()) {
                                    dbRawIterator.next();
                                    continue;
                                }

                                processWhen1stColSatisfied(indexKey)?;

                                dbRawIterator.next()
                            }
                        }
                        Op::MathCmpOp(MathCmpOp::LessEqual) => {
                            dbRawIterator.seek_for_prev(buffer.as_ref());

                            loop {
                                let indexKey = getKeyIfSome!(dbRawIterator);
                                processWhen1stColSatisfied(indexKey)?;

                                dbRawIterator.prev();
                            }
                        }
                        Op::MathCmpOp(MathCmpOp::LessThan) => {
                            dbRawIterator.seek_for_prev(buffer.as_ref());

                            loop {
                                let indexKey = getKeyIfSome!(dbRawIterator);

                                if indexKey.starts_with(buffer.as_ref()) {
                                    dbRawIterator.prev();
                                    continue;
                                }

                                processWhen1stColSatisfied(indexKey)?;

                                dbRawIterator.prev();
                            }
                        }
                        _ => panic!("impossible")
                    }
                }
            }
        }

        Ok(())
    }

    // 对and来说  前边的column已经满足了 还需要进1步测试
    // 对or来说 不会调用该函数了 因为 要使用index的话 表的过滤条件的字段只能单个 且 要是 index的打头字段
    fn further(&self, indexKey: &[Byte], indexSearch: &IndexSearch) -> anyhow::Result<Option<IndexSearchResult>> {
        // key的末尾是dataKey
        let dataKey = extractDataKeyFromIndexKey!(indexKey);

        // 对index以表数据读取
        let indexRowData = &indexKey[..(indexKey.len() - meta::DATA_KEY_BYTE_LEN)];
        let mut myBytesRowData = MyBytes::from(Bytes::from(Vec::from(indexRowData)));
        let columnValues = Vec::try_from(&mut myBytesRowData)?;

        // index用到的只有1个的column
        if indexSearch.opValueVecVecAcrossIndexFilteredCols.len() == 1 {
            if indexSearch.indexLocalSearch {
                match self.indexLocalSearch(columnValues, dataKey, indexSearch)? {
                    Some(rowData) => return Ok(Some(IndexSearchResult::Direct((dataKey, rowData)))),
                    None => return Ok(None)
                }
            }

            return Ok(Some(IndexSearchResult::Redirect(dataKey)));
        }

        // 因为table的filter可能不会用光index上的全部的字段
        let remainingIndexColValues = &columnValues[1..=indexSearch.opValueVecVecAcrossIndexFilteredCols.len()];

        let opValueVecVecOnRemaingIndexCols = &indexSearch.opValueVecVecAcrossIndexFilteredCols[1..];

        // opValueVecOnRemaingIndexCols 之间 or
        for (remainingIndexColValue, opValueVecVecOnRemaingIndexCol) in remainingIndexColValues.iter().zip(opValueVecVecOnRemaingIndexCols) {
            let mut satisfyOneOpValueVec = false;

            // 元素之间 是 and 还是 or 取决 isAnd
            'opValueVecVecOnRemaingIndexCol:
            for opValueVecOnRemaingIndexCol in opValueVecVecOnRemaingIndexCol {
                for (op, value) in opValueVecOnRemaingIndexCol {
                    let satisfy = remainingIndexColValue.calcOneToOne(*op, value)?.asBoolean()?;
                    if indexSearch.isAnd {
                        if satisfy == false {
                            // 切换到下个 opValueVec
                            continue 'opValueVecVecOnRemaingIndexCol;
                        }
                    } else {
                        if satisfy {
                            return Ok(Some(IndexSearchResult::Redirect(dataKey)));
                        }
                    }
                }

                if indexSearch.isAnd { // 如果是and 到了这边 说明 opValueVecOnRemaingIndexCol 上的筛选全都通过了(它们之间是and)
                    satisfyOneOpValueVec = true;
                    break 'opValueVecVecOnRemaingIndexCol;
                }
            }

            if indexSearch.isAnd {
                // 当前这个的column上彻底失败了
                if satisfyOneOpValueVec == false {
                    return Ok(None);
                }
            }
        }

        if indexSearch.isAnd {
            if indexSearch.indexLocalSearch {
                match self.indexLocalSearch(columnValues, dataKey, indexSearch)? {
                    Some(rowData) => return Ok(Some(IndexSearchResult::Direct((dataKey, rowData)))),
                    None => return Ok(None)
                }
            }

            Ok(Some(IndexSearchResult::Redirect(dataKey)))
        } else {
            Ok(None)
        }
    }

    /// 调用该函数的时候已然是通过了 filter的测试 还需要通过mvcc visibility测试
    fn indexLocalSearch(&self,
                        columnValues: Vec<GraphValue>, // index上的全部的column的data
                        datakey: DataKey,
                        indexSearch: &IndexSearch) -> Result<Option<RowData>> {
        let index = indexSearch.dbObjectIndex.asIndex()?;

        // mvcc visibility筛选
       // if self.checkCommittedDataVisiWithoutTxMutations(&mut mvccKeyBuffer,
         //                                                &mut rawIterator,
           //                                              dataKey,
             //                                            &columnFamily,
               //                                          &scanParams.table.name)? == false {
          //  return Ok(None);
        //}

        // 以上是全都在已落地的维度内的visibility check
        // 还要结合当前事务上的尚未提交的mutations,看已落地的是不是应该干掉
        //if let Some(mutationsRawCurrentTx) = tableMutationsCurrentTx {
          //  if self.checkCommittedDataVisiWithTxMutations(mutationsRawCurrentTx, &mut mvccKeyBuffer, dataKey)? == false {
            //    return Ok(None);
            //}
        //}


        let mut rowData: RowData = HashMap::with_capacity(index.columnNames.len());

        for (columnName, columnValue) in index.columnNames.iter().zip(columnValues) {
            rowData.insert(columnName.clone(), columnValue);
        }

        let rowData = store::pruneRowData(rowData, indexSearch.selectedColNames)?;
        Ok(Some(rowData))
    }
}

pub(in crate::executor) enum IndexSearchResult {
    Direct((DataKey, RowData)),
    Redirect(DataKey),
}
