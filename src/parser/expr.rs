use crate::expr::Expr;
use crate::{global, prefix_plus_plus, suffix_plus_plus};
use crate::parser::element::Element;
use crate::parser::op::{Op, SqlOp};
use crate::parser::Parser;
use anyhow::Result;

impl Parser {
    // todo 能够应对like
    /// 当link sql解析到表名后边的"("时候 调用该函数 不过调用的时候elementIndex还是"("的前边1个 <br>
    /// stopWhenParseRightComplete 用来应对(a>0+6),0+6未被括号保护,不然的话会解析成  (a>1)+6
    pub(super) fn parseExpr(&mut self, stopWhenParseRightComplete: bool) -> Result<Expr> {
        // 像 a = 0+1 的 0+1 是没有用括号保护起来的 需要有这样的标识的
        let mut hasLeading括号 = false;

        if self.getCurrentElement()?.expectTextLiteralContentBool(global::圆括号_STR) {
            hasLeading括号 = true;
            self.skipElement(1)?;
        }

        enum ParseCondState {
            ParsingLeft,
            ParsingOp,
            ParsingRight,
            ParseRightComplete,
        }

        let mut expr = Expr::default();

        let mut parseCondState = ParseCondState::ParsingLeft;

        loop {
            let currentElement = match self.getCurrentElementAdvanceOption() {
                None => break,
                Some(currentElement) => currentElement.clone(),
            };

            match parseCondState {
                ParseCondState::ParsingLeft => {
                    if currentElement.expectTextLiteralContentBool(global::圆括号_STR) {
                        self.skipElement(-1)?;
                        expr = self.parseExpr(false)?;
                        parseCondState = ParseCondState::ParsingOp;
                        continue;
                    }

                    expr = Expr::Single(currentElement);

                    parseCondState = ParseCondState::ParsingOp;
                }
                ParseCondState::ParsingOp => {
                    if let Element::Op(op) = currentElement {
                        expr = Expr::BiDirection {
                            leftExpr: Box::new(expr),
                            op,
                            rightExprs: Default::default(),
                        }
                    } else {
                        // 应对 (((a = 1)))  因为递归结束后返回到上1级的时候currentElement是")"
                        // if currentElement.expectTextLiteralContentBool(global::括号1_STR) {
                        //   break;
                        //}

                        parseCondState = ParseCondState::ParseRightComplete;
                        self.skipElement(-1)?;
                        continue;
                        // self.throwSyntaxError()?;
                    }

                    parseCondState = ParseCondState::ParsingRight;
                }
                ParseCondState::ParsingRight => {
                    match currentElement {
                        Element::TextLiteral(ref text) => {
                            // 后续要支持 a in ('a') 和 a = (0+1)
                            if text == global::圆括号_STR {
                                // 要应对 a in ('a'),那么碰到"("的话需要去看看前边的是不是 in

                                // 需要先回过去然后回过来,不然prevElement还是currentElement
                                self.skipElement(-1)?;
                                let previousElement = self.peekPrevElement()?.clone();
                                self.skipElement(1)?;

                                // 说明是 "... in ( ..." 这样的,括号对应的便不是单个expr而是多个expr
                                if let Element::Op(Op::SqlOp(SqlOp::In)) = previousElement {
                                    self.skipElement(-1)?;

                                    // 得要BiDirection
                                    if let Expr::BiDirection { leftExpr: left, op, .. } = expr {
                                        expr = Expr::BiDirection {
                                            leftExpr: left,
                                            op,
                                            rightExprs: self.parseInExprs()?.into_iter().map(|expr| { Box::new(expr) }).collect(),
                                        }
                                    } else {
                                        self.throwSyntaxError()?;
                                    }
                                } else if let Element::Op(_) = previousElement { // 前边是别的op
                                    self.skipElement(-1)?;

                                    // 递归
                                    let subExpr = self.parseExpr(false)?;

                                    // 得要BiDirection
                                    if let Expr::BiDirection { leftExpr: left, op, .. } = expr {
                                        expr = Expr::BiDirection {
                                            leftExpr: left,
                                            op,
                                            rightExprs: vec![Box::new(subExpr)],
                                        }
                                    } else {
                                        self.throwSyntaxError()?;
                                    }
                                } else {
                                    self.throwSyntaxError()?;
                                }

                                parseCondState = ParseCondState::ParseRightComplete;
                                continue;
                            }
                        }
                        _ => {}
                    }

                    if let Expr::BiDirection { leftExpr: left, op, .. } = expr {
                        expr = Expr::BiDirection {
                            leftExpr: left,
                            op,
                            rightExprs: vec![Box::new(Expr::Single(currentElement))],
                        }
                    } else {
                        self.throwSyntaxError()?;
                    }

                    parseCondState = ParseCondState::ParseRightComplete;
                }
                ParseCondState::ParseRightComplete => {
                    match currentElement {
                        Element::TextLiteral(text) => {
                            // 要是不是以(打头的话,那么到这没有必要继续了
                            if hasLeading括号 == false {
                                self.skipElement(-1)?;
                                break;
                            }

                            // (a = 1) 的 ")",说明要收了，递归结束要返回上轮
                            if text == global::圆括号1_STR {
                                break;
                            }

                            // 别的情况要报错
                        }
                        Element::Op(op) => {
                            // 需要区分 原来是都是认为是logicalOp
                            match op {
                                // 它是之前能应对的情况 a = 1 and b= 0 的 and
                                // (a and b or d) 会解析变成 a and (b or d) 不对 应该是 (a and b) or d
                                Op::LogicalOp(_) => {
                                    if stopWhenParseRightComplete {
                                        // 不要遗忘
                                        self.skipElement(-1)?;
                                        break;
                                    }

                                    // getCurrentElement()其实已是下个了
                                    let nextElementIs括号 = self.getCurrentElement()?.expectTextLiteralContentBool(global::圆括号_STR);

                                    expr = Expr::BiDirection {
                                        leftExpr: Box::new(expr),
                                        op,
                                        // 需要递归下钻
                                        rightExprs: vec![Box::new(self.parseExpr(!nextElementIs括号)?)],
                                    };
                                    // (m and (a = 0 and (b = 1))) 这个时候解析到的是1后边的那个")"而已 还有")"残留
                                    // (a=0 and (b=1) and 1 or 0)
                                    parseCondState = ParseCondState::ParseRightComplete;
                                    continue;
                                }
                                // a>0+6 and b=0 的 "+",当前的expr是a>0,需要打破现有的expr
                                Op::MathCalcOp(_) => {
                                    if let Expr::BiDirection { leftExpr: left, op, .. } = expr {
                                        // 需要先回到0+6的起始index
                                        self.skipElement(-2)?;

                                        expr = Expr::BiDirection {
                                            leftExpr: left,
                                            op,
                                            // 递归的level不能用力太猛 不然应对不了 a > 0+6 and b=0 会把 0+6 and b=0 当成1个expr
                                            rightExprs: vec![Box::new(self.parseExpr(true)?)],
                                        };

                                        parseCondState = ParseCondState::ParseRightComplete;
                                        continue;
                                    }
                                }
                                // 0+6>a and b=0的 ">" 当前的expr是0+6
                                Op::MathCmpOp(_) => {
                                    // 把现有的expr降级变为小弟
                                    expr = Expr::BiDirection {
                                        leftExpr: Box::new(expr),
                                        op,
                                        rightExprs: Default::default(),
                                    };
                                    // 不递归而是本level循环
                                    parseCondState = ParseCondState::ParsingRight;
                                    continue;
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }

                    self.throwSyntaxError()?;
                }
            }
        }

        Ok(expr)
    }

    /// 单独的函数解析 a in (0,0+6,0+(a+1),)的in后边的括号包含的用","分隔的多个expr
    // 单独的生成小的parser,element只包含expr的
    pub(super) fn parseInExprs(&mut self) -> Result<Vec<Expr>> {
        // 要以(打头
        self.getCurrentElement()?.expectTextLiteralContent(global::圆括号_STR)?;

        let mut 括号count = 0;
        let mut 括号1count = 0;

        let mut pendingElementVec = Vec::new();
        let mut exprParserVec = Vec::new();
        let mut exprVec = Vec::new();

        loop {
            // 通过","分隔提取
            let currentElement = self.getCurrentElementAdvance()?;

            match currentElement {
                Element::TextLiteral(text) => {
                    match text.as_str() {
                        global::圆括号_STR => {
                            pendingElementVec.push(currentElement.clone());

                            suffix_plus_plus!(括号count);
                        }
                        // 要以)收尾
                        global::圆括号1_STR => {
                            // 说明括号已然收敛了 是last的)
                            if prefix_plus_plus!(括号1count) == 括号count {
                                // pending的不要忘了
                                if pendingElementVec.len() > 0 {
                                    let mut exprParser = Parser::default();
                                    exprParser.elementVecVec.push(pendingElementVec);
                                    exprParserVec.push(exprParser);
                                    // pendingElementVec = Vec::new();
                                }
                                break;
                            } else {
                                // 当不是last的)可以添加
                                pendingElementVec.push(currentElement.clone());
                            }
                        }
                        global::逗号_STR => {
                            if pendingElementVec.len() == 0 {
                                continue;
                            }

                            let mut exprParser = Parser::default();
                            exprParser.elementVecVec.push(pendingElementVec);
                            exprParserVec.push(exprParser);
                            pendingElementVec = Vec::new();
                        }
                        _ => pendingElementVec.push(currentElement.clone()),
                    }
                }
                _ => pendingElementVec.push(currentElement.clone()),
            }
        }

        for exprParser in &mut exprParserVec {
            exprVec.push(exprParser.parseExpr(false)?);
        }

        Ok(exprVec)
    }
}