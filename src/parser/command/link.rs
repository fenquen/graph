use serde::{Deserialize, Serialize};
use crate::{global, suffix_plus_plus};
use crate::expr::Expr;
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::op::{MathCmpOp, Op};
use crate::parser::Parser;

// todo 后续要改变当前的relation保存体系
/// link user(id = 1) to car(color = 'red') by usage(number = 2)
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Link {
    pub srcTableName: String,
    pub srcTableFilterExpr: Option<Expr>,

    pub destTableName: String,
    pub destTableFilterExpr: Option<Expr>,

    pub relationName: String,
    pub relationColumnNames: Vec<String>,
    pub relationColumnExprs: Vec<Expr>,
    pub relationFilterExpr: Option<Expr>,
}

impl Parser {
    // link user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13)
    // todo 能不能实现 ```link user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre```
    pub(in crate::parser) fn parseLink(&mut self, regardLastPartAsFilter: bool) -> anyhow::Result<Command> {
        let mut link = Link::default();

        #[derive(Clone, Copy)]
        enum ParseSrcDestState {
            ParseSrcTableName,
            ParseSrcTableCondition,
            ParseDestTableName,
            ParseDestTableCondition,
        }

        let mut parseSrcDestState = ParseSrcDestState::ParseSrcTableName;

        loop {
            let text = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?;
            match (parseSrcDestState, text.to_uppercase().as_str()) {
                (ParseSrcDestState::ParseSrcTableName, _) => {
                    link.srcTableName = text;
                    parseSrcDestState = ParseSrcDestState::ParseSrcTableCondition;
                }
                (ParseSrcDestState::ParseSrcTableCondition, global::圆括号_STR) => {
                    // 返回1个确保当前的element是"("
                    self.skipElement(-1)?;
                    link.srcTableFilterExpr = Some(self.parseExpr(false)?);

                    parseSrcDestState = ParseSrcDestState::ParseDestTableName;
                }
                // src table的筛选条件不存在
                (ParseSrcDestState::ParseSrcTableCondition, "TO") => {
                    self.skipElement(-1)?;
                    parseSrcDestState = ParseSrcDestState::ParseDestTableName;
                }
                (ParseSrcDestState::ParseDestTableName, "TO") => {
                    link.destTableName = self.getCurrentElementAdvance()?.expectTextLiteral("to should followed by dest table name when use link sql")?;
                    parseSrcDestState = ParseSrcDestState::ParseDestTableCondition;
                }
                (ParseSrcDestState::ParseDestTableCondition, global::圆括号_STR) => {
                    self.skipElement(-1)?;
                    link.destTableFilterExpr = Some(self.parseExpr(false)?);
                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("by", "missing 'by'")?;

        link.relationName = self.getCurrentElementAdvance()?.expectTextLiteral("relation name")?;

        if regardLastPartAsFilter {
            let nextElement = self.getCurrentElementOption();
            if nextElement.is_none() {
                return Ok(Command::Link(link));
            }
            let nextElement = nextElement.unwrap();

            nextElement.expectTextLiteralContent(global::圆括号_STR)?;

            link.relationFilterExpr = Some(self.parseExpr(false)?);
        } else {
            // 下边要解析 by usage (a=0,a=(1212+0))的后边的括号部分了
            // 和parseInExprs使用相同的套路,当(数量和)数量相同的时候说明收敛结束了,因为会以")"收尾
            let mut 括号数量 = 0;
            let mut 括号1数量 = 0;
            if let Some(currentElement) = self.getCurrentElementAdvanceOption() {
                currentElement.expectTextLiteralContent(global::圆括号_STR)?;
                suffix_plus_plus!(括号数量);
            } else { // 未写link的value
                return Ok(Command::Link(link));
            }

            #[derive(Clone, Copy)]
            enum ParseState {
                ParseColumnName,
                ParseEqual,
                ParseColumnExpr,
            }

            let mut parseState = ParseState::ParseColumnName;
            let mut exprElementVec = Default::default();
            loop {
                let currentElement =
                    match self.getCurrentElementAdvanceOption() {
                        Some(currentElement) => currentElement,
                        None => break,
                    };

                match (parseState, currentElement) {
                    (ParseState::ParseColumnName, Element::TextLiteral(columnName)) => {
                        link.relationColumnNames.push(columnName.to_string());
                        parseState = ParseState::ParseEqual;
                    }
                    (ParseState::ParseEqual, Element::Op(Op::MathCmpOp(MathCmpOp::Equal))) => {
                        parseState = ParseState::ParseColumnExpr;
                    }
                    (ParseState::ParseColumnExpr, currentElement) => {
                        // 说明右半部分的expr结束了
                        if currentElement.expectTextLiteralContentBool(global::逗号_STR) {
                            let mut parser = Parser::default();
                            parser.elementVecVec.push(exprElementVec);
                            link.relationColumnExprs.push(parser.parseExpr(false)?);
                            exprElementVec = Default::default();

                            // 到下轮的parseColumnName
                            parseState = ParseState::ParseColumnName;
                            continue;
                        }

                        if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                            suffix_plus_plus!(括号数量);
                        } else if currentElement.expectTextLiteralContentBool(global::圆括号1_STR) {
                            suffix_plus_plus!(括号1数量);

                            // 说明到了last的)
                            if 括号数量 == 括号1数量 {
                                let mut parser = Parser::default();
                                parser.elementVecVec.push(exprElementVec);
                                link.relationColumnExprs.push(parser.parseExpr(false)?);
                                // exprElementVec = Default::default();
                                break;
                            }
                        }

                        exprElementVec.push(currentElement.clone());
                    }
                    _ => self.throwSyntaxError()?,
                }
            }

            // relation的name和value数量要相同
            if link.relationColumnNames.len() != link.relationColumnExprs.len() {
                self.throwSyntaxErrorDetail("relation name count does not match value count")?;
            }
        }

        Ok(Command::Link(link))
    }
}