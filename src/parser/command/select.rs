use std::collections::HashSet;
use std::ops::Bound;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::{global, throw};
use crate::graph_error::GraphError;
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::op::{MathCalcOp, Op, SqlOp};
use crate::parser::Parser;
use crate::types::RelationDepth;
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
pub enum Select {
    SelectTable(SelectTable),
    SelectRels(Vec<SelectRel>),
    SelectTableUnderRels(SelectTableUnderRels),
}

// todo select的时候column也有alias
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SelectTable {
    pub tableName: String,
    pub selectedColNames: Option<Vec<String>>,
    pub tableFilterExpr: Option<Expr>,
    pub tableAlias: Option<String>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SelectRel {
    pub srcTableName: String,
    pub srcColumnNames: Option<Vec<String>>,
    pub srcFilter: Option<Expr>,
    pub srcAlias: Option<String>,

    pub relationName: Option<String>,
    pub relationColumnNames: Option<Vec<String>>,
    pub relationFilter: Option<Expr>,
    pub relationDepth: Option<RelationDepth>,
    pub relationAlias: Option<String>,

    pub destTableName: Option<String>,
    pub destColumnNames: Option<Vec<String>>,
    pub destFilter: Option<Expr>,
    pub destAlias: Option<String>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SelectTableUnderRels {
    pub selectTable: SelectTable,
    pub relDescVec: Vec<RelDesc>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct RelDesc {
    /// 该node处在rel的哪个位置上
    pub endPointType: EndPointType,
    pub relationName: String,
    pub relationFliter: Option<Expr>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub enum EndPointType {
    Start,
    #[default]
    Either,
    End,
}

impl FromStr for EndPointType {
    type Err = GraphError;

    fn from_str(str: &str) -> std::result::Result<Self, Self::Err> {
        match str.to_lowercase().as_str() {
            "start" => Ok(EndPointType::Start),
            "either" => Ok(EndPointType::Either),
            "end" => Ok(EndPointType::End),
            _ => throw!("unknown"),
        }
    }
}

impl Parser {
    // todo 实现 select user(id >1 ) as user0 ,in usage (number = 7) ,end in own(number =7)
    /// ```select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre```
    pub(in crate::parser) fn parseSelect(&mut self) -> anyhow::Result<Command> {
        // https://doc.rust-lang.org/reference/items/enumerations.html
        #[repr(u8)]
        #[derive(Clone, Copy, PartialEq, PartialOrd)]
        enum State {
            ReadSrcName = 0, // 必有
            ReadSrcColumnNames, // 可选
            ReadSrcFilterExpr, // 可选
            ReadSrcAlias, // 可选

            ReadRelationName, // 可选
            ReadRelationColumnNames,// 可选
            ReadRelationFilterExpr,// 可选
            ReadRelationDepth,
            ReadRelationAlias,// 可选

            ReadDestName,// 可选
            ReadDestColumnNames,// 可选
            ReadDestFilterExpr,// 可选
            ReadDestAlias,// 可选

            TryNextRound,
        }

        fn parseSelectedColumnNames(parser: &mut Parser) -> Result<Vec<String>> {
            let mut columnNames = Vec::default();

            loop {
                let text = parser.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;

                match text.as_str() {
                    global::逗号_STR => continue,
                    global::方括号1_STR => break,
                    _ => columnNames.push(text),
                }
            }

            if columnNames.is_empty() {
                parser.throwSyntaxErrorDetail("no explicit column name")?;
            }

            Ok(columnNames)
        }

        let mut state = State::ReadSrcName;
        // getCurrentElement可不可是None
        let mut force = true;

        let mut selectRelVec = Vec::new();
        let mut selectRel = SelectRel::default();

        loop {
            let currentElement =
                if force {
                    self.getCurrentElementAdvance()?
                } else {
                    if let Some(currentElement) = self.getCurrentElementAdvanceOption() {
                        currentElement
                    } else {
                        break;
                    }
                };

            match state {
                State::ReadSrcName => {
                    selectRel.srcTableName = currentElement.expectTextLiteral("expect src table name")?;

                    state = State::ReadSrcColumnNames;
                    force = false;
                }
                State::ReadSrcColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        selectRel.srcColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadSrcFilterExpr;
                    force = false;
                }
                State::ReadSrcFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        selectRel.srcFilter = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadSrcAlias;
                    force = false;
                }
                State::ReadSrcAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        selectRel.srcAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by src alias")?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationName;
                    force = false;
                }
                State::ReadRelationName => {
                    if let Element::Op(Op::MathCalcOp(MathCalcOp::Minus)) = currentElement {
                        selectRel.relationName = Some(self.getCurrentElementAdvance()?.expectTextLiteral("expect a relation name")?);
                    } else { // 未写 realition 那么后边的全部都不会有了
                        break;
                    }

                    state = State::ReadRelationColumnNames;
                    force = false;
                }
                State::ReadRelationColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        selectRel.relationColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationFilterExpr;
                    force = false;
                }
                State::ReadRelationFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        selectRel.relationFilter = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationDepth;
                    force = false;
                }
                //  todo 实现递归搜索
                State::ReadRelationDepth => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("recursive") {
                        // 使用独立的mini模式
                        let mut parseMini = Parser::default();

                        let mut elementVec = Vec::new();

                        // 收集需要的元素
                        loop {
                            // a
                            let element = self.getCurrentElementAdvance()?;
                            elementVec.push(element.clone());

                            if let Element::TextLiteral(text) = element {
                                match text.as_str() {
                                    global::方括号1_STR | global::圆括号1_STR => break,
                                    _ => {}
                                }
                            }
                        }

                        parseMini.elementVecVec.push(elementVec);

                        selectRel.relationDepth = Some(parseMini.parseRelationDepth()?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationAlias;
                    force = false;
                }
                State::ReadRelationAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        selectRel.relationAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by relation alias")?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestName;
                    force = true;
                }
                State::ReadDestName => {
                    if let Element::To = currentElement {
                        selectRel.destTableName = Some(self.getCurrentElementAdvance()?.expectTextLiteral("expect a relation name")?);

                        // 如果对relation使用recursive的话 起点和终点都要是相同的table
                        if let Some(_) = selectRel.relationDepth {
                            if selectRel.srcTableName.as_str() != selectRel.destTableName.as_ref().unwrap() {
                                self.throwSyntaxErrorDetail("when use relation recursive query, start,end node should belong to same table")?;
                            }
                        }
                    } else {
                        break;
                    }

                    state = State::ReadDestColumnNames;
                    force = false;
                }
                State::ReadDestColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        selectRel.destColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestFilterExpr;
                    force = false;
                }
                State::ReadDestFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        selectRel.destFilter = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestAlias;
                    force = false;
                }
                State::ReadDestAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        selectRel.destAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by dest alias")?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::TryNextRound;
                    force = false;
                }
                State::TryNextRound => { // 尝试读取下个rel打头的 -rel-> 的 minus部分
                    if let Element::Op(Op::MathCalcOp(MathCalcOp::Minus)) = currentElement {
                        self.skipElement(-1)?;

                        // https://qastack.cn/programming/19650265/is-there-a-faster-shorter-way-to-initialize-variables-in-a-rust-struct
                        let selectRel0 = SelectRel {
                            srcTableName: selectRel.destTableName.as_ref().unwrap().clone(),
                            srcColumnNames: selectRel.destColumnNames.clone(),
                            srcFilter: selectRel.destFilter.clone(),
                            srcAlias: selectRel.destAlias.clone(),
                            ..Default::default()
                        };

                        selectRelVec.push(selectRel);

                        selectRel = selectRel0;

                        state = State::ReadRelationName;
                        force = false;
                    } else {
                        break;
                    }
                }
            }
        }

        // 说明只是读个table而已
        if (State::ReadSrcName..=State::ReadRelationName).contains(&state) {
            let selectTable = SelectTable {
                tableName: selectRel.srcTableName,
                selectedColNames: selectRel.srcColumnNames,
                tableFilterExpr: selectRel.srcFilter,
                tableAlias: selectRel.srcAlias,
            };

            // 还要区分
            match state {
                // 读取relName的是没有下文了 符合 selectTableUnderRels
                State::ReadRelationName => {
                    // 复用成果 因为前部分都是select 1个 表
                    self.skipElement(-1)?;
                    return self.parseSelectTableUnderRels(selectTable);
                }
                // 对应[State::ReadSrcName, State::ReadRelationName)
                _ => return Ok(Command::Select(Select::SelectTable(selectTable))),
            }
        }

        selectRelVec.push(selectRel);

        // 确保alias不能重复
        let mut existAlias: HashSet<String> = HashSet::new();

        let mut testDuplicatedAlias =
            |alias: Option<&String>| {
                if alias.is_some() {
                    if existAlias.insert(alias.unwrap().to_string()) == false {
                        self.throwSyntaxErrorDetail(&format!("duplicated alias:{}", alias.unwrap()))?;
                    }
                }

                Result::<(), anyhow::Error>::Ok(())
            };

        for select in &selectRelVec {
            testDuplicatedAlias(select.srcAlias.as_ref())?;
            testDuplicatedAlias(select.relationAlias.as_ref())?;
            testDuplicatedAlias(select.destAlias.as_ref())?;
        }

        Ok(Command::Select(Select::SelectRels(selectRelVec)))
    }

    /// ```select user(id >1 ) as user0 ,in usage (number = 7) ,as end in own(number =7)```
    fn parseSelectTableUnderRels(&mut self, selectTable: SelectTable) -> Result<Command> {
        let mut selectTableUnderRels = SelectTableUnderRels::default();

        // 复用成果
        selectTableUnderRels.selectTable = selectTable;

        // 开始的时候 当前的element应该是"," 先消耗
        self.getCurrentElementAdvance()?.expectTextLiteralContent(global::逗号_STR)?;

        enum State {
            ReadEndPointType,
            ReadRelName,
            ReadRelFilterExpr,
        }

        let mut state = State::ReadEndPointType;

        // 读取1个relDesc的小loop
        loop {
            let relDesc = {
                let mut relDesc = RelDesc::default();

                loop {
                    let currentElement = self.getCurrentElementAdvanceOption();
                    if let None = currentElement {
                        return Ok(Command::Select(Select::SelectTableUnderRels(selectTableUnderRels)));
                    }
                    let currentElement = currentElement.unwrap().clone();

                    match state {
                        State::ReadEndPointType => {
                            let mut foundAs = false;
                            // as start in
                            if currentElement.expectTextLiteralContentBool("as") {
                                let nextElement = self.getCurrentElementAdvance()?;
                                let s = nextElement.expectTextLiteral(global::EMPTY_STR)?;
                                relDesc.endPointType = EndPointType::from_str(s.as_str())?;

                                foundAs = true;
                            }

                            // 读取 in
                            match if foundAs { self.getCurrentElementAdvance()?.clone() } else { currentElement } {
                                Element::Op(Op::SqlOp(SqlOp::In)) => {}
                                _ => self.throwSyntaxErrorDetail("in should be before relation name")?,
                            }

                            state = State::ReadRelName;
                        }
                        State::ReadRelName => {
                            relDesc.relationName = currentElement.expectTextLiteral("need a relation name")?;
                            state = State::ReadRelFilterExpr;
                        }
                        State::ReadRelFilterExpr => {
                            if currentElement.expectTextLiteralContentIgnoreCaseBool(global::圆括号_STR) {
                                self.skipElement(-1)?;
                                relDesc.relationFliter = Some(self.parseExpr(false)?);
                            }

                            if let Some(element) = self.getCurrentElementAdvanceOption() {
                                // start a new round
                                if global::逗号_STR == element.expectTextLiteral(global::EMPTY_STR)? {
                                    state = State::ReadEndPointType;
                                } else {
                                    self.throwSyntaxErrorDetail("need comma after a relation desc")?;
                                }
                            }

                            break;
                        }
                    }
                }

                relDesc
            };

            selectTableUnderRels.relDescVec.push(relDesc);
        }
    }

    /// 使用的是mini模式 <br>
    ///  ```(..1) [1..2] (1..2] [1..2)```
    fn parseRelationDepth(&mut self) -> Result<RelationDepth> {
        let mut elementVec = Vec::new();

        // 之前的parseElement时候像 1..2 和 7.. 和 ..3 和 .. 都会当成1个全体成为textLiteral
        // 需要在这边对其内容进行进1步的拆分 把这个textLiteral变为 integerLiteral textLiteral integerLiteral
        for element in &self.elementVecVec[self.currentElementVecIndex] {
            if let Element::TextLiteral(text) = element {
                match text.as_str() {
                    global::方括号1_STR |
                    global::方括号1_STR |
                    global::圆括号_STR |
                    global::圆括号1_STR => elementVec.push(element.clone()),
                    _ => { // 具体拆分
                        let mut pendingChars = Vec::new();

                        let mut readingNumber = None;

                        let mut chars = text.chars();
                        loop {
                            match chars.next() {
                                Some(char) => {
                                    match char {
                                        '0'..='9' => {
                                            // 之前读的都是非数字 需要收集了
                                            if let Some(false) = readingNumber {
                                                if pendingChars.is_empty() == false {
                                                    elementVec.push(Element::TextLiteral(pendingChars.iter().collect::<String>()));
                                                    pendingChars.clear();
                                                }
                                            }

                                            pendingChars.push(char);
                                            readingNumber = Some(true);
                                        }
                                        _ => {
                                            // 之前读的都是数字 需要收集
                                            if let Some(true) = readingNumber {
                                                if pendingChars.is_empty() == false {
                                                    elementVec.push(Element::IntegerLiteral(pendingChars.iter().collect::<String>().as_str().parse::<i64>()?));
                                                    pendingChars.clear();
                                                }
                                            }

                                            pendingChars.push(char);
                                            readingNumber = Some(false);
                                        }
                                    }
                                }
                                None => { // 读到末尾了 要将残留的收拢
                                    if let Some(readingNumber) = readingNumber {
                                        if pendingChars.is_empty() == false {
                                            if readingNumber {
                                                elementVec.push(Element::IntegerLiteral(pendingChars.iter().collect::<String>().as_str().parse::<i64>()?));
                                            } else {
                                                elementVec.push(Element::TextLiteral(pendingChars.iter().collect::<String>()));
                                            }
                                        }
                                    }

                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                elementVec.push(element.clone());
            }
        }

        // 重新组织
        self.elementVecVec[self.currentElementVecIndex] = elementVec;

        let startBound = {
            let currentElement = self.getCurrentElementAdvance()?;

            let exclusive =
                match currentElement.expectTextLiteral(&format!("expect a text literal however got a {:?}", currentElement))?.as_str() {
                    global::方括号_STR => false,
                    global::圆括号_STR => true,
                    _ => self.throwSyntaxError()?,
                };

            // getCurrentElement() 已然是next了
            // 说明写了显式的depth 而且的话是这样写的 (1 .. 6) depth和后边的两个dot是有分隔的
            //
            // 如果没有分隔的话 会连成1起变为textLiteral
            if let Some(startDepth) = self.getCurrentElement()?.expectIntegerLiteralOpt() {
                if 0 >= startDepth {
                    self.throwSyntaxErrorDetail("relation start depth should > 0")?;
                }

                self.skipElement(1)?;

                match (exclusive, startDepth) {
                    (false, _) => Bound::Included(startDepth as usize),
                    (true, _) => Bound::Included((startDepth + 1) as usize), // depth是整数可以转换到Include
                }
            } else {
                // 原来的话写的是unbound 要是起点的话和include 1 相同
                Bound::Included(1)
            }
        };

        // 后边要是2个的dot
        self.getCurrentElementAdvance()?.expectTextLiteralContent("..")?;

        let endBound = {
            let endDepth =
                if let Some(endDepth) = self.getCurrentElement()?.expectIntegerLiteralOpt() {
                    if 0 >= endDepth {
                        self.throwSyntaxErrorDetail("relation end depth should > 0")?;
                    }

                    self.skipElement(1)?;

                    Some(endDepth)
                } else {
                    None
                };

            // 读取了末尾的
            let currentElement = self.getCurrentElementAdvance()?;
            match (endDepth, currentElement.expectTextLiteral(&format!("expect a text literal however got a {:?}", currentElement))?.as_str()) {
                (Some(endDepth), global::方括号1_STR) => Bound::Included(endDepth as usize),
                (Some(endDepth), global::圆括号1_STR) => Bound::Included((endDepth - 1) as usize),
                (None, global::方括号1_STR | global::圆括号1_STR) => Bound::Unbounded,
                (_, _) => self.throwSyntaxErrorDetail("")?,
            }
        };

        match (startBound, endBound) {
            (Bound::Included(startDepth), Bound::Included(endDepth)) => {
                if startDepth > endDepth {
                    self.throwSyntaxErrorDetail(&format!("[{},{}] is not allowed", startDepth, endDepth))?;
                }
            }
            (Bound::Included(startDepth), Bound::Excluded(endDepth)) => {
                if startDepth >= endDepth {
                    self.throwSyntaxErrorDetail(&format!("[{},{}) is not allowed", startDepth, endDepth))?;
                }
            }
            (Bound::Excluded(startDepth), Bound::Included(endDepth)) => {
                if startDepth >= endDepth {
                    self.throwSyntaxErrorDetail(&format!("({},{}] is not allowed", startDepth, endDepth))?;
                }
            }
            (Bound::Excluded(startDepth), Bound::Excluded(endDepth)) => {
                // 两边都是exculde的话
                if startDepth >= endDepth || endDepth - startDepth == 1 {
                    self.throwSyntaxErrorDetail(&format!("({},{}) is not allowed", startDepth, endDepth))?;
                }
            }
            _ => {}
        }

        Ok((startBound, endBound))
    }
}