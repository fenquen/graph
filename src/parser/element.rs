use std::fmt::{Debug, Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::{global, suffix_plus_plus, throw};
use crate::parser::op::{LogicalOp, MathCalcOp, Op, SqlOp};
use crate::parser::Parser;
use anyhow::Result;
use strum_macros::Display as StrumDisplay;
use crate::types::ElementType;

#[derive(Clone, Serialize, Deserialize)]
pub enum Element {
    /// 如果只有TextLiteral的话 还是不能区分 (')') 的两个右括号的
    TextLiteral(String),
    /// 对应''包括起来的内容
    StringContent(String),
    IntegerLiteral(i64),
    DecimalLiteral(f64),
    Op(Op),
    Boolean(bool),
    /// 对应"->"
    Arrow2Right,
    Null,
    Not,
    Default,
}

impl Element {
    pub const TEXT_LITERAL: ElementType = 0;
    pub const STRING_CONTENT: ElementType = 1;
    pub const INTEGER_LITERAL: ElementType = 2;
    pub const DECIMAL_LITERAL: ElementType = 3;
    pub const OP: ElementType = 4;
    pub const BOOLEAN: ElementType = 5;
    pub const ARROW_2_RIGHT: ElementType = 6;
    pub const NULL: ElementType = 7;
    pub const NOT: ElementType = 8;
    pub const DEFAULT: ElementType = 9;

    pub(super) fn getType(&self) -> ElementType {
        match self {
            Element::TextLiteral(_) => Self::TEXT_LITERAL,
            Element::StringContent(_) => Self::STRING_CONTENT,
            Element::IntegerLiteral(_) => Self::INTEGER_LITERAL,
            Element::DecimalLiteral(_) => Self::DECIMAL_LITERAL,
            Element::Op(_) => Self::OP,
            Element::Boolean(_) => Self::BOOLEAN,
            Element::Arrow2Right => Self::ARROW_2_RIGHT,
            Element::Null => Self::NULL,
            Element::Not => Self::NOT,
            Element::Default => Self::DEFAULT,
        }
    }

    pub(super) fn expectTextLiteralOpt(&self) -> Option<String> {
        if let Element::TextLiteral(text) = self {
            Some(text.to_string())
        } else {
            None
        }
    }

    pub(super) fn expectTextLiteral(&self, errorStr: &str) -> Result<String> {
        if let Some(text) = self.expectTextLiteralOpt() {
            Ok(text.to_string())
        } else {
            throw!(&format!("expect Element::TextLiteral but get {:?}, {}", self, errorStr))
        }
    }

    pub(super) fn expectTextLiteralSilent(&self) -> Result<String> {
        if let Some(text) = self.expectTextLiteralOpt() {
            Ok(text.to_string())
        } else {
            throw!(&format!("expect Element::TextLiteral but get {:?}", self))
        }
    }

    pub(super) fn expectTextLiteralContent(&self, expectContent: &str) -> Result<()> {
        if self.expectTextLiteralContentBool(expectContent) {
            Ok(())
        } else {
            throw!(&format!("expect Element::TextLiteral({}) but get {:?}", expectContent, self))
        }
    }

    pub(super) fn expectTextLiteralContentBool(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            content == expectContent
        } else {
            false
        }
    }

    pub(super) fn expectTextLiteralContentIgnoreCaseBool(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            let expectContent = expectContent.to_uppercase();
            let content = content.to_uppercase();

            expectContent == content
        } else {
            false
        }
    }

    pub(super) fn expectTextLiteralContentIgnoreCase(&self, expectContent: &str, errorStr: &str) -> Result<()> {
        if self.expectTextLiteralContentIgnoreCaseBool(expectContent) {
            Ok(())
        } else {
            throw!(errorStr)
        }
    }

    #[inline]
    pub(super) fn expectTextLiteralContentIgnoreCaseSilent(&self, expectContent: &str) -> Result<()> {
        self.expectTextLiteralContentIgnoreCase(expectContent, global::EMPTY_STR)
    }

    pub(super) fn expectIntegerLiteralOpt(&self) -> Option<i64> {
        if let Element::IntegerLiteral(number) = self {
            Some(*number)
        } else {
            None
        }
    }

    pub(super) fn expectIntegerLiteral(&self) -> Result<i64> {
        if let Some(number) = self.expectIntegerLiteralOpt() {
            Ok(number)
        } else {
            throw!(&format!("expect integer literal however got: {:?}", self))
        }
    }
}

impl Display for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Element::TextLiteral(s) => write!(f, "TextLiteral({})", s),
            Element::StringContent(s) => write!(f, "StringContent({})", s),
            Element::IntegerLiteral(s) => write!(f, "IntegerLiteral({})", s),
            Element::DecimalLiteral(s) => write!(f, "DecimalLiteral({})", s),
            Element::Boolean(bool) => write!(f, "Boolean({})", bool),
            Element::Op(op) => write!(f, "Op({})", op),
            Element::Arrow2Right => write!(f, "To"),
            Element::Null => write!(f, "Null"),
            Element::Not => write!(f, "Not"),
            Element::Default => write!(f, "Default")
            // _ => write!(f, "unknown")
        }
    }
}

impl Debug for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Parser {
    pub(super) fn parseElement(&mut self) -> anyhow::Result<()> {
        let mut currentElementVec: Vec<Element> = Vec::new();

        let mut 括号数量: usize = 0;
        let mut 括号1数量: usize = 0;

        let ascInvisibleCodeRange = char::from(0u8)..=char::from(31);

        // 空格 逗号 单引号 括号
        loop {
            let mut advanceCount: usize = 1;

            // "insert   INTO TEST VALUES ( ',',1 )"
            let currentChar = self.getCurrentChar();
            match currentChar {
                // 空格如果不是文本内容的话不用记录抛弃
                global::SPACE_CHAR => {
                    // 是不是文本本身的内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        self.collectPendingChars(&mut currentElementVec);
                    }
                }
                global::单引号_CHAR => {
                    if self.whetherIn单引号() {
                        match self.peekNextChar() {
                            // 说明是末尾了,下边的文本结束是相同的 select a where name = 'a'
                            None => {
                                self.collectPendingChars(&mut currentElementVec);

                                self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                            }
                            Some(nextChar) => {
                                // 连续的2个 单引号 对应1个
                                if nextChar == global::单引号_CHAR {
                                    self.pendingChars.push(currentChar);
                                    advanceCount = 2;
                                } else { // 说明文本结束的
                                    self.collectPendingChars(&mut currentElementVec);

                                    self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                                }
                            }
                        }
                    } else {
                        // 开启了1个文本读取 需要把老的了结掉
                        self.collectPendingChars(&mut currentElementVec);

                        self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                    }
                }
                global::圆括号_CHAR | global::圆括号1_CHAR |
                global::逗号_CHAR |
                global::方括号_CHAR | global::方括号1_CHAR => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        self.collectPendingChars(&mut currentElementVec);

                        // 本身也添加到elementVec
                        currentElementVec.push(Element::TextLiteral(currentChar.to_string()));

                        match currentChar {
                            global::圆括号_CHAR | global::方括号_CHAR => 括号数量 = 括号数量 + 1,
                            global::圆括号1_CHAR | global::方括号1_CHAR => 括号1数量 = 括号1数量 + 1,
                            _ => {}
                        }
                    }
                }
                global::分号_CHAR | global::换行_CHAR => { // 要是写了多个sql的话 以";" 和 换行分割
                    // 单纯的是文本内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else { // 说明是多个的sql的分隔的 到了1个sql的收尾
                        self.collectPendingChars(&mut currentElementVec);

                        if currentElementVec.len() > 0 {
                            self.elementVecVec.push(currentElementVec);
                            currentElementVec = Vec::new();
                        }
                    }
                }
                // 数学比较符 因为是可以粘连的 需要到这边来parse
                global::等号_CHAR | global::小于_CHAR | global::大于_CHAR | global::感叹_CHAR => {
                    // 单纯currentCharIndex的是文本内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        let operatorString: String =
                            // 应对  "!=" ">=" "<=" 两个char的 目前的不容许有空格的
                            if let Some(nextChar) = self.peekNextChar() {
                                match nextChar {
                                    global::等号_CHAR | global::小于_CHAR | global::大于_CHAR | global::感叹_CHAR => {
                                        advanceCount = 2;
                                        vec![currentChar, nextChar].iter().collect()
                                    }
                                    // 还是1元的operator
                                    _ => vec![currentChar].iter().collect(),
                                }
                            } else {
                                vec![currentChar].iter().collect()
                            };

                        // parse内部调用的还是MathCmpOp::from_str
                        let mathCmpOp = operatorString.as_str().parse()?;

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);

                        currentElementVec.push(Element::Op(Op::MathCmpOp(mathCmpOp)));
                    }
                }
                // 数学计算符 因为是可以粘连的 需要到这边来parse
                global::加号_CHAR | global::除号_CHAR | global::乘号_CHAR | global::减号_CHAR => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        // 应对->
                        let element = if currentChar == global::减号_CHAR && Some(global::大于_CHAR) == self.peekNextChar() {
                            advanceCount = 2;
                            Element::Arrow2Right
                        } else {
                            let mathCalcOp = MathCalcOp::fromChar(currentChar)?;
                            Element::Op(Op::MathCalcOp(mathCalcOp))
                        };

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(element);
                    }
                }
                'n' | 'N' => { // 应对null, not
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                        continue;
                    }

                    if self.tryPrefecthIgnoreCase(&vec!['U', 'L', 'L']) {
                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(Element::Null);
                    } else if self.tryPrefecthIgnoreCase(&vec!['o', 't']) {
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(Element::Not);
                    } else {
                        self.pendingChars.push(currentChar);
                    }
                }
                'd' | 'D' => { // 应对default
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                        continue;
                    }

                    if self.tryPrefecthIgnoreCase(&vec!['e', 'f', 'a', 'u', 'l', 't']) {
                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(Element::Default);
                    } else {
                        self.pendingChars.push(currentChar);
                    }
                }
                'l' | 'L' => {  // todo 实现对like的parse 完成
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                        continue;
                    }

                    if self.tryPrefecthIgnoreCase(&vec!['i', 'k', 'e']) {
                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(Element::Op(Op::SqlOp(SqlOp::Like)));
                    } else {
                        self.pendingChars.push(currentChar);
                    }
                }
                _ => {
                    // ascii的不可见char应该抛弃 和处理空格的是不同的 空格会认为是element的分隔的 a
                    if ascInvisibleCodeRange.contains(&currentChar) == false || self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    }
                }
            }

            let reachEnd = self.advanceChar(advanceCount);
            if reachEnd {
                self.collectPendingChars(&mut currentElementVec);

                if currentElementVec.is_empty() == false {
                    self.elementVecVec.push(currentElementVec);
                }

                break;
            }
        }

        // 需要确保单引号 和括号是对称的
        if self.whetherIn单引号() || 括号数量 != 括号1数量 {
            self.throwSyntaxError()?;
        }

        if self.elementVecVec.len() == 0 {
            self.throwSyntaxErrorDetail("the sql is empty string")?;
        }

        Ok(())
    }

    /// 要确保调用前还未涉入到要测试的range
    fn tryPrefecthIgnoreCase(&mut self, targetChars: &[char]) -> bool {
        let currentCharIndexCopy = self.currentCharIndex;

        for targetChar in targetChars {
            // 到了末尾
            if self.advanceChar(1) {
                self.currentCharIndex = currentCharIndexCopy;
                return false;
            }

            let currentChar = self.getCurrentChar();
            let current = vec![currentChar].iter().collect::<String>().to_uppercase();

            let target = vec![*targetChar].iter().collect::<String>().to_uppercase();

            if current != target {
                self.currentCharIndex = currentCharIndexCopy;
                return false;
            }
        }

        // 看看后边是不是还有粘连的(是不是只是某个长的textLiteral的前1部分)
        match self.peekNextChar() {
            Some(nextChar) => {
                match nextChar {
                    // 说明是某个长的textLiteral的前1部分
                    'a'..='z' => {
                        self.currentCharIndex = currentCharIndexCopy;
                        return false;
                    }
                    'A'..='Z' => {
                        self.currentCharIndex = currentCharIndexCopy;
                        return false;
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        true
    }

    /// 要是会已到末尾以外 返回true
    /// 和下边element体系不同 用光了的话index还是指向last元素的 还是不太优秀的
    fn advanceChar(&mut self, count: usize) -> bool {
        if self.currentCharIndex + count >= self.sql.len() {
            self.currentCharIndex = self.sql.len() - 1;
            return true;
            // throw!("当前已是sql的末尾不能advance了");
        }

        self.currentCharIndex = self.currentCharIndex + count;

        false
    }

    /// 和下边element体系不同 用光了的话index还是指向last元素的 还是不太优秀的
    fn getCurrentChar(&self) -> char {
        self.chars[self.currentCharIndex]
    }

    /// peek而已不会变化currentCharIndex
    fn peekPrevChar(&self) -> Option<char> {
        if self.currentCharIndex == 0 {
            None
        } else {
            Some(self.chars[self.currentCharIndex - 1])
        }
    }

    /// peek而已不会变化currentCharIndex
    fn peekNextChar(&self) -> Option<char> {
        if self.currentCharIndex + 1 >= self.sql.len() {
            None
        } else {
            Some(self.chars[self.currentCharIndex + 1])
        }
    }

    fn collectPendingChars(&mut self, dest: &mut Vec<Element>) {
        let text: String = self.pendingChars.iter().collect();

        // 如果是空字符的话还是需要记录的
        if text == global::EMPTY_STR {
            if self.whetherIn单引号() == false {
                return;
            }
        }

        let (isPureNumberText, isDecimal) = Parser::isPureNumberText(&text);

        let element =
            // text是纯数字
            if isPureNumberText {
                // 当前是不是在单引号的包围 是文本
                if self.whetherIn单引号() {
                    Element::StringContent(text)
                } else {
                    if isDecimal {
                        Element::DecimalLiteral(text.parse::<f64>().unwrap())
                    } else {
                        Element::IntegerLiteral(text.parse::<i64>().unwrap())
                    }
                }
            } else {
                if self.whetherIn单引号() {
                    Element::StringContent(text)
                } else {
                    // parse bool的时候文本的bool不能大小写混合的
                    // 以下的op能够到这边解析的原因是,它们是不能粘连使用的,然而"+"是可以的, 0+2 和0 + 2 都是对的
                    // 故而它们是不能到这边解析,需要和mathCmpOp那样到循环里边去parse
                    match text.to_uppercase().as_str() {
                        "FALSE" => Element::Boolean(false),
                        "TRUE" => Element::Boolean(true),
                        "OR" => Element::Op(Op::LogicalOp(LogicalOp::Or)),
                        "AND" => Element::Op(Op::LogicalOp(LogicalOp::And)),
                        "IN" => Element::Op(Op::SqlOp(SqlOp::In)),
                        // todo 应对 recursive 查询
                        _ => {
                            Element::TextLiteral(text)
                        }
                    }
                }
            };

        dest.push(element);
        self.pendingChars.clear();
    }

    fn whetherIn单引号(&self) -> bool {
        self.单引号as文本边界的数量 % 2 != 0
    }

    fn isPureNumberText(text: &str) -> (bool, bool) {
        if text.len() == 0 {
            return (false, false);
        }

        if text == global::DOT_STR {
            return (false, false);
        }

        let mut hasMetDot = false;
        let mut dotIndex: i32 = -1;

        let mut currentIndex = 0;

        for char in text.chars() {
            match char {
                '0'..='9' => continue,
                global::DOT_CHAR => {
                    // 可以是打头和末尾 不能有多个的
                    if hasMetDot { // 说明有多个的
                        return (false, false);
                    }

                    hasMetDot = true;
                    dotIndex = currentIndex;
                }
                '-' => {
                    // 需要打头
                    if currentIndex != 0 {
                        return (false, false);
                    }
                }
                _ => return (false, false),
            }

            currentIndex = currentIndex + 1;
        }

        // 如果'.'是打头的那么是小数 不然integer
        let decimal = if hasMetDot {
            // 小数
            if dotIndex != text.len() as i32 - 1 {
                true
            } else {
                false
            }
        } else {
            false
        };

        (true, decimal)
    }

    /// 返回None的话说明当前已经是overflow了 和之前遍历char时候不同的是 当不能advance时候index是在最后的index还要向后1个的
    pub(super) fn getCurrentElementAdvanceOption(&mut self) -> Option<&Element> {
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex);
        if option.is_some() {
            suffix_plus_plus!(self.currentElementIndex);
        }
        option
    }

    /// getCurrentElementAdvance, 得到current element 然后 advance
    pub(super) fn getCurrentElementAdvance(&mut self) -> Result<&Element> {
        if let Some(element) = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex) {
            suffix_plus_plus!(self.currentElementIndex);
            Ok(element)
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    pub(super) fn getCurrentElement(&self) -> Result<&Element> {
        if let Some(element) = self.getCurrentElementOption() {
            Ok(element)
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    pub(super) fn getCurrentElementOption(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex)
    }

    /// 和 peekNextElement 不同的是 得到的是 Option 不是 result
    pub(super) fn peekPrevElementOpt(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex - 1)
    }

    pub(super) fn peekPrevElement(&self) -> Result<&Element> {
        if let Some(previousElement) = self.peekPrevElementOpt() {
            Ok(previousElement)
        } else {
            self.throwSyntaxError()?
        }
    }

    pub(super) fn peekNextElementOpt(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex + 1)
    }

    pub(super) fn peekNextElement(&self) -> Result<&Element> {
        if let Some(nextElement) = self.peekNextElementOpt() {
            Ok(nextElement)
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    /// 和parse toke 遍历char不同的是 要是越界了 index会是边界的后边1个 以符合当前的体系
    pub(super) fn skipElement(&mut self, delta: i64) -> Result<()> {
        let currentElementVecLen = self.elementVecVec.get(self.currentElementVecIndex).unwrap().len();

        if (self.currentElementIndex as i64 + delta) as usize >= currentElementVecLen {
            self.currentElementIndex = currentElementVecLen;
            self.throwSyntaxError()
        } else {
            self.currentElementIndex = (self.currentElementIndex as i64 + delta) as usize;
            Ok(())
        }
    }

    pub(super) fn resetCurrentElementIndex(&mut self) {
        self.currentElementIndex = 0;
    }

    pub(super) fn hasRemainingElement(&self) -> bool {
        self.getCurrentElementOption().is_some()
    }
}