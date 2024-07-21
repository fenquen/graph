use serde::{Deserialize, Serialize};
use crate::{global, suffix_plus_plus};
use crate::expr::Expr;
use crate::parser::command::Command;
use crate::parser::element::Element;
use crate::parser::op::{MathCmpOp, Op};
use crate::parser::Parser;
use anyhow::Result;
use crate::parser::command::select::SelectRel;

#[derive(Debug, Serialize, Deserialize)]
pub enum Link {
    LinkTo(LinkTo),
    LinkChain(Vec<SelectRel>),
}

/// link user(id = 1) to car(color = 'red') by usage(number = 2)
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct LinkTo {
    pub srcTableName: String,
    pub srcTableFilter: Option<Expr>,

    pub destTableName: String,
    pub destTableFilter: Option<Expr>,

    pub relationName: String,
    pub relationColumnNames: Vec<String>,
    pub relationColumnExprs: Vec<Expr>,

    /// 给unlink用的
    pub relationFilter: Option<Expr>,
}

impl Parser {
    pub(in crate::parser) fn parseLink(&mut self, regardRelPartAsFilter: bool) -> Result<Command> {
        // 简单粗暴的先检验elemnt里边有没有->
        let hasToArrow = {
            let mut hasToArrow = false;

            'outer:
            for elementVec in &self.elementVecVec {
                for element in elementVec {
                    if let Element::To = element {
                        hasToArrow = true;
                        break 'outer;
                    }
                }
            }

            hasToArrow
        };

        if hasToArrow {
            self.parseLinkChain()
        } else {
            self.parseLinkTo(regardRelPartAsFilter)
        }
    }

    ///  link user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 12)
    fn parseLinkTo(&mut self, regardLastPartAsFilter: bool) -> Result<Command> {
        let mut linkToStyle = LinkTo::default();

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
                    linkToStyle.srcTableName = text;
                    parseSrcDestState = ParseSrcDestState::ParseSrcTableCondition;
                }
                (ParseSrcDestState::ParseSrcTableCondition, global::圆括号_STR) => {
                    // 返回1个确保当前的element是"("
                    self.skipElement(-1)?;
                    linkToStyle.srcTableFilter = Some(self.parseExpr(false)?);

                    parseSrcDestState = ParseSrcDestState::ParseDestTableName;
                }
                // src table的筛选条件不存在
                (ParseSrcDestState::ParseSrcTableCondition, "TO") => {
                    self.skipElement(-1)?;
                    parseSrcDestState = ParseSrcDestState::ParseDestTableName;
                }
                (ParseSrcDestState::ParseDestTableName, "TO") => {
                    linkToStyle.destTableName = self.getCurrentElementAdvance()?.expectTextLiteral("to should followed by dest table name when use link sql")?;
                    parseSrcDestState = ParseSrcDestState::ParseDestTableCondition;
                }
                (ParseSrcDestState::ParseDestTableCondition, global::圆括号_STR) => {
                    self.skipElement(-1)?;
                    linkToStyle.destTableFilter = Some(self.parseExpr(false)?);
                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("by", "missing 'by'")?;

        linkToStyle.relationName = self.getCurrentElementAdvance()?.expectTextLiteral("relation name")?;

        // 如果是true的话是用在unlink上,relation名字后边的括号是对relation的筛选条件而不是用来set的value
        if regardLastPartAsFilter {
            // 单纯的去瞧瞧next element 不去advance
            let nextElement = {
                let nextElement = self.getCurrentElementOption();

                if nextElement.is_none() {
                    return Ok(Command::Link(Link::LinkTo(linkToStyle)));
                }

                nextElement.unwrap()
            };

            nextElement.expectTextLiteralContent(global::圆括号_STR)?;

            linkToStyle.relationFilter = Some(self.parseExpr(false)?);
        } else { // 应对的是建立关系的时候, 相当于是insert
            (linkToStyle.relationColumnNames, linkToStyle.relationColumnExprs) = self.parseRelInsertValues()?;
        }

        Ok(Command::Link(Link::LinkTo(linkToStyle)))
    }

    /// link user(id=1 and 0=6) -usage(number = 9) -> car -own(number=1)-> tyre
    #[inline]
    fn parseLinkChain(&mut self) -> Result<Command> {
        self.parseSelect(false)
    }

    /// 应对 link to 体系中的 by usage (a=0, a=(1212+0)),它其实是对relation的insert values
    pub(super) fn parseRelInsertValues(&mut self) -> Result<(Vec<String>, Vec<Expr>)> {
        // 和parseInExprs使用相同的套路,当(数量和)数量相同的时候说明收敛结束了,因为会以")"收尾
        let mut relationColumnNames = Vec::new();
        let mut relationColumnExprs = Vec::new();

        let mut 括号数量 = 0usize;
        let mut 括号1数量 = 0usize;

        if let Some(currentElement) = self.getCurrentElementAdvanceOption() {
            currentElement.expectTextLiteralContent(global::圆括号_STR)?;
            suffix_plus_plus!(括号数量);
        } else { // 未写link的value
            return Ok((relationColumnNames, relationColumnExprs));
        }

        #[derive(Clone, Copy)]
        enum ParseState {
            ParseColumnName,
            ParseEqual,
            ParseColumnExpr,
        }

        let mut parseState = ParseState::ParseColumnName;
        let mut exprElementVec = Vec::new();
        loop {
            let currentElement =
                match self.getCurrentElementAdvanceOption() {
                    Some(currentElement) => currentElement,
                    None => break,
                };

            match (parseState, currentElement) {
                (ParseState::ParseColumnName, Element::TextLiteral(columnName)) => {
                    relationColumnNames.push(columnName.to_string());
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
                        relationColumnExprs.push(parser.parseExpr(false)?);
                        exprElementVec = Vec::new();

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
                            relationColumnExprs.push(parser.parseExpr(false)?);
                            break;
                        }
                    }

                    exprElementVec.push(currentElement.clone());
                }
                _ => self.throwSyntaxError()?,
            }
        }

        // relation的name和value数量要相同
        if relationColumnNames.len() != relationColumnExprs.len() {
            self.throwSyntaxErrorDetail("relation name count does not match value count")?;
        }

        Ok((relationColumnNames, relationColumnExprs))
    }
}