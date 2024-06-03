use std::collections::HashSet;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::{global, throw};
use crate::graph_error::GraphError;
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::op::{MathCalcOp, Op, SqlOp};
use crate::parser::Parser;

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
    pub srcFilterExpr: Option<Expr>,
    pub srcAlias: Option<String>,

    pub relationName: Option<String>,
    pub relationColumnNames: Option<Vec<String>>,
    pub relationFliterExpr: Option<Expr>,
    pub relationAlias: Option<String>,

    pub destTableName: Option<String>,
    pub destColumnNames: Option<Vec<String>>,
    pub destFilterExpr: Option<Expr>,
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
            ReadRelationAlias,// 可选

            ReadDestName,// 可选
            ReadDestColumnNames,// 可选
            ReadDestFilterExpr,// 可选
            ReadDestAlias,// 可选

            TryNextRound,
        }

        fn parseSelectedColumnNames(parser: &mut Parser) -> anyhow::Result<Vec<String>> {
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

        let mut selectVec = Vec::default();
        let mut select = SelectRel::default();

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
                    select.srcTableName = currentElement.expectTextLiteral("expect src table name")?;

                    state = State::ReadSrcColumnNames;
                    force = false;
                }
                State::ReadSrcColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        select.srcColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadSrcFilterExpr;
                    force = false;
                }
                State::ReadSrcFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        select.srcFilterExpr = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadSrcAlias;
                    force = false;
                }
                State::ReadSrcAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        select.srcAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by src alias")?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationName;
                    force = false;
                }
                State::ReadRelationName => {
                    if let Element::Op(Op::MathCalcOp(MathCalcOp::Minus)) = currentElement {
                        select.relationName = Some(self.getCurrentElementAdvance()?.expectTextLiteral("expect a relation name")?);
                    } else { // 未写 realition 那么后边的全部都不会有了
                        break;
                    }

                    state = State::ReadRelationColumnNames;
                    force = false;
                }
                State::ReadRelationColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        select.relationColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationFilterExpr;
                    force = false;
                }
                State::ReadRelationFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        select.relationFliterExpr = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadRelationAlias;
                    force = false;
                }
                State::ReadRelationAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        select.relationAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by relation alias")?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestName;
                    force = true;
                }
                State::ReadDestName => {
                    if let Element::To = currentElement {
                        select.destTableName = Some(self.getCurrentElementAdvance()?.expectTextLiteral("expect a relation name")?);
                    } else {
                        break;
                    }

                    state = State::ReadDestColumnNames;
                    force = false;
                }
                State::ReadDestColumnNames => {
                    if currentElement.expectTextLiteralContentBool(global::方括号_STR) {
                        select.destColumnNames = Some(parseSelectedColumnNames(self)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestFilterExpr;
                    force = false;
                }
                State::ReadDestFilterExpr => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        select.destFilterExpr = Some(self.parseExpr(false)?);
                    } else {
                        self.skipElement(-1)?;
                    }

                    state = State::ReadDestAlias;
                    force = false;
                }
                State::ReadDestAlias => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool("as") {
                        select.destAlias = Some(self.getCurrentElementAdvance()?.expectTextLiteral("as should followed by dest alias")?);
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
                        let select0 = SelectRel {
                            srcTableName: select.destTableName.as_ref().unwrap().clone(),
                            srcColumnNames: select.destColumnNames.clone(),
                            srcFilterExpr: select.destFilterExpr.clone(),
                            srcAlias: select.destAlias.clone(),
                            ..Default::default()
                        };

                        selectVec.push(select);

                        select = select0;

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
                tableName: select.srcTableName,
                selectedColNames: select.srcColumnNames,
                tableFilterExpr: select.srcFilterExpr,
                tableAlias: select.srcAlias,
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

        selectVec.push(select);

        // 确保alias不能重复
        let mut existAlias: HashSet<String> = HashSet::new();

        let mut testDuplicatedAlias = |alias: Option<&String>| {
            if alias.is_some() {
                if existAlias.insert(alias.unwrap().to_string()) == false {
                    self.throwSyntaxErrorDetail(&format!("duplicated alias:{}", alias.unwrap()))?;
                }
            }

            anyhow::Result::<(), anyhow::Error>::Ok(())
        };

        for select in &selectVec {
            testDuplicatedAlias(select.srcAlias.as_ref())?;
            testDuplicatedAlias(select.relationAlias.as_ref())?;
            testDuplicatedAlias(select.destAlias.as_ref())?;
        }

        Ok(Command::Select(Select::SelectRels(selectVec)))
    }

    /// ```select user(id >1 ) as user0 ,in usage (number = 7) ,as end in own(number =7)```
    fn parseSelectTableUnderRels(&mut self, selectTable: SelectTable) -> anyhow::Result<Command> {
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
}