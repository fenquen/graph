use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::expr::Expr;
use crate::global;
use crate::parser::command::Command;
use crate::parser::command::link::Link;
use crate::parser::command::select::{EndPointType, RelDesc};
use crate::parser::element::Element;
use crate::parser::op::{Op, SqlOp};
use crate::parser::Parser;

pub type UnlinkLinkStyle = Link;

#[derive(Debug, Serialize, Deserialize)]
pub enum Unlink {
    LinkStyle(UnlinkLinkStyle),
    SelfStyle(UnlinkSelfStyle),
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct UnlinkSelfStyle {
    pub tableName: String,
    pub tableFilterExpr: Option<Expr>,
    pub relDescVec: Vec<RelDesc>,
}

impl Parser {
    /// unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13) <br>
    /// todo unlink user(id >1 ) as start in usage (number = 7) ,as end in own(number =7) 感觉不该用到unlink上,反而应该用到select上
    pub(in crate::parser) fn parseUnlink(&mut self) -> anyhow::Result<Command> {
        // 尝试先用link的套路parse
        if let Ok(Command::Link(link)) = self.parseLink(true) {
            return Ok(Command::Unlink(Unlink::LinkStyle(link)));
        }

        let mut unlinkSelfStyle = UnlinkSelfStyle::default();

        // 说明不是link的书写模式,重置index
        self.currentElementIndex = 0;

        // 跳过打头的unlink
        self.getCurrentElementAdvance()?;

        enum State {
            ReadEndPointName,
            ReadEndPointFilterExpr,

            ReadEndPointType,
            ReadRelName,
            ReadRelFilterExpr,
        }

        let mut state = State::ReadEndPointName;

        // 循环1趟的小loop 单单读取 tableName table的filter
        loop {
            let currentElement = self.getCurrentElementAdvanceOption();
            if let None = currentElement {
                return Ok(Command::Unlink(Unlink::SelfStyle(unlinkSelfStyle)));
            }
            let currentElement = currentElement.unwrap().clone();

            match state {
                State::ReadEndPointName => {
                    unlinkSelfStyle.tableName = currentElement.expectTextLiteral("need a node name")?;
                    state = State::ReadEndPointFilterExpr;
                }
                State::ReadEndPointFilterExpr => {
                    if currentElement.expectTextLiteralContentIgnoreCaseBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        unlinkSelfStyle.tableFilterExpr = Some(self.parseExpr(false)?);
                    }

                    state = State::ReadEndPointType;
                    break;
                }
                _ => panic!("impossible")
            }
        }


        // 读取1个relDesc的小loop
        loop {
            let relDesc = {
                let mut relDesc = RelDesc::default();

                loop {
                    let currentElement = self.getCurrentElementAdvanceOption();
                    if let None = currentElement {
                        return Ok(Command::Unlink(Unlink::SelfStyle(unlinkSelfStyle)));
                    }
                    let currentElement = currentElement.unwrap().clone();

                    match state {
                        State::ReadEndPointType => {
                            // as start in
                            if currentElement.expectTextLiteralContentBool("as") {
                                let nextElement = self.getCurrentElementAdvance()?;
                                let s = nextElement.expectTextLiteral(global::EMPTY_STR)?;
                                relDesc.endPointType = EndPointType::from_str(s.as_str())?;
                            }

                            // 读取 in
                            match self.getCurrentElementAdvance()? {
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
                        _ => panic!("impossible"),
                    }
                }

                relDesc
            };

            unlinkSelfStyle.relDescVec.push(relDesc);
        }
    }
}