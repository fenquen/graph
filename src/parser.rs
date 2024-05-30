use std::cmp::PartialEq;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter, Pointer, write};
use std::ops::RangeToInclusive;
use std::str::FromStr;
use crate::{global, prefix_plus_plus, suffix_minus_minus, suffix_plus_plus, throw};
use anyhow::Result;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use strum_macros::{Display as DisplayStrum, Display, EnumString};
use crate::expr::Expr;
use crate::graph_error::GraphError;
use crate::graph_value::PointDesc;
use crate::meta::{Column, ColumnType, Table, TableType};

pub fn parse(sql: &str) -> Result<Vec<Command>> {
    if sql.is_empty() {
        return Ok(vec![]);
    }

    let mut parser = Parser::new(sql);

    parser.parseElement()?;

    for elementVec in &parser.elementVecVec {
        for element in elementVec {
            println!("{element}");
        }

        println!();
    }

    parser.parse()
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    CreateTable(Table),
    Insert(Insert),
    Link(Link),
    Update(Update),
    Select(Select),
    Delete(Delete),
    Unlink(Unlink),
}

impl Command {
    pub fn isDml(&self) -> bool {
        match self {
            Command::Insert(_) | Command::Link(_) | Command::Update(_) | Command::Unlink(_) => true,
            _ => false
        }
    }
}

#[derive(Default)]
pub struct Parser {
    sql: String,

    chars: Vec<char>,
    currentCharIndex: usize,
    pendingChars: Vec<char>,
    单引号as文本边界的数量: usize,

    /// 因为可能会1趟写多个使用;分隔的sql 也会有多个Vec<Element>
    elementVecVec: Vec<Vec<Element>>,
    /// ``` Vec<Vec<Element>>的index```
    currentElementVecIndex: usize,
    /// ```Vec<Element>的index```
    currentElementIndex: usize,
}

impl Parser {
    pub fn new(sql: &str) -> Self {
        let mut parser = Parser::default();
        parser.sql = sql.to_string();
        parser.chars = parser.sql.chars().collect::<Vec<char>>();

        parser
    }

    pub fn clear(&mut self) {
        self.sql.clear();

        self.chars.clear();
        self.currentCharIndex = 0;
        self.pendingChars.clear();
        self.单引号as文本边界的数量 = 0;

        self.elementVecVec.clear();
        self.currentElementVecIndex = 0;
        self.currentElementIndex = 0;
    }

    fn parseElement(&mut self) -> Result<()> {
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
                            Element::To
                        } else {
                            let mathCalcOp = MathCalcOp::fromChar(currentChar)?;
                            Element::Op(Op::MathCalcOp(mathCalcOp))
                        };

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(element);
                    }
                }
                // 应对null
                'n' | 'N' => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        if self.tryPrefecthIgnoreCase(&vec!['U', 'L', 'L']) {
                            // 需要了断 pendingChars
                            self.collectPendingChars(&mut currentElementVec);
                            currentElementVec.push(Element::Null);
                        } else {
                            self.pendingChars.push(currentChar);
                        }
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
                self.elementVecVec.push(currentElementVec);
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
                break;
            }

            let currentChar = self.getCurrentChar();
            let current = vec![currentChar].iter().collect::<String>().to_uppercase();

            let target = vec![*targetChar].iter().collect::<String>().to_uppercase();

            if current != target {
                self.currentCharIndex = currentCharIndexCopy;
                return false;
            }
        }

        // 看看后边是不是还有粘连的 是不是只是某个长的textLiteral的前1部分
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
                        _ => Element::TextLiteral(text),
                    }
                }
            };

        dest.push(element);
        self.pendingChars.clear();
    }

    fn whetherIn单引号(&self) -> bool {
        self.单引号as文本边界的数量 % 2 != 0
    }

    fn throwSyntaxError<T>(&self) -> Result<T> {
        throw!(&format!("syntax error, sql:{}", self.sql))
    }

    fn throwSyntaxErrorDetail<T>(&self, message: &str) -> Result<T> {
        throw!(&format!("syntax error, sql:{}, {}", self.sql, message))
    }

    fn isPureNumberText(text: &str) -> (bool, bool) {
        if text.len() == 0 {
            return (false, false);
        }

        if text == "." {
            return (false, false);
        }

        let mut hasMetDot = false;
        let mut dotIndex: i32 = -1;

        let mut currentIndex = 0;

        for char in text.chars() {
            match char {
                '0'..='9' => continue,
                '.' => {
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

    fn parse(&mut self) -> Result<Vec<Command>> {
        let mut commandVec = Vec::new();

        loop {
            let command = match self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase().as_str() {
                "CREATE" => self.parseCreate()?,
                "INSERT" => self.parseInsert()?,
                "LINK" => self.parseLink(false)?,
                "DELETE" => self.parseDelete()?,
                "UPDATE" => self.parseUpdate()?,
                "SELECT" => self.parseSelect()?,
                "UNLINK" => self.parseUnlink()?,
                _ => self.throwSyntaxError()?,
            };

            println!("{:?}\n", command);

            commandVec.push(command);

            // 到下个的elementVec
            if prefix_plus_plus!(self.currentElementVecIndex) >= self.elementVecVec.len() {
                break;
            }

            self.currentElementIndex = 0;
        }

        Ok(commandVec)
    }

    // todo 实现 default value
    // todo 实现 if not exist 完成
    // CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)
    fn parseCreate(&mut self) -> Result<Command> {
        // 不是table便是relation
        let tableType = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase().as_str().parse()?;
        self.parseCreateTable(tableType)
    }

    fn parseCreateTable(&mut self, tableType: TableType) -> Result<Command> {
        let mut table = Table::default();

        // 应对 if not exist
        if self.getCurrentElement()?.expectTextLiteralContentIgnoreCaseBool("if") {
            self.skipElement(1)?;

            let errMessage = "you should wirte \"if not exist\" after create table";
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("not", errMessage)?;
            self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("exist", errMessage)?;

            table.createIfNotExist = true;
        }

        table.type0 = tableType;

        // 读取table name
        table.name = self.getCurrentElementAdvance()?.expectTextLiteral("table name can not be pure number")?;

        // table名不能胡乱
        self.checkDbObjectName(&table.name)?;

        // 应该是"("
        self.getCurrentElementAdvance()?.expectTextLiteralContent(global::圆括号_STR)?;

        // 循环读取 column
        enum ReadColumnState {
            ReadColumnName,
            ReadColumnType,
            ReadComplete,
        }

        let mut readColumnState = ReadColumnState::ReadColumnName;
        let mut column = Column::default();
        loop {
            let element = self.getCurrentElementAdvanceOption();
            if element.is_none() {
                break;
            }

            let element = element.unwrap();
            match element {
                Element::TextLiteral(text) => {
                    // 砍断和text->element->&mut self联系 不然下边的throwSyntaxErrorDetail报错因为是&self的
                    let text = text.to_string();

                    match readColumnState {
                        ReadColumnState::ReadColumnName => {
                            self.checkDbObjectName(&text)?;
                            column.name = text;
                            readColumnState = ReadColumnState::ReadColumnType;
                        }
                        ReadColumnState::ReadColumnType => {
                            column.type0 = text.as_str().parse()?;
                            readColumnState = ReadColumnState::ReadComplete;

                            // 应对 null
                            // 读取下个element
                            if let Element::Null = self.getCurrentElement()? {
                                self.skipElement(1)?;
                                column.nullable = true;
                            }
                        }
                        ReadColumnState::ReadComplete => {
                            match text.as_str() {
                                global::逗号_STR => {
                                    readColumnState = ReadColumnState::ReadColumnName;

                                    table.columns.push(column);
                                    column = Column::default();

                                    continue;
                                }
                                global::圆括号1_STR => {
                                    table.columns.push(column);
                                    break;
                                }
                                _ => self.throwSyntaxError()?,
                            }
                        }
                    }
                }
                _ => self.throwSyntaxErrorDetail("column name, column type can not be pure number")?,
            }
        }

        Ok(Command::CreateTable(table))
    }

    // insert   INTO TEST VALUES ( '0'  , ')')
    // insert into test (column1) values ('a')
    // todo 实现 insert into values(),()
    fn parseInsert(&mut self) -> Result<Command> {
        let currentElement = self.getCurrentElementAdvance()?;
        if currentElement.expectTextLiteralContentIgnoreCaseBool("into") == false {
            self.throwSyntaxErrorDetail("insert should followed by into")?;
        }

        let mut insertValues = Insert::default();

        insertValues.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("table name should not pure number")?.to_string();

        loop { // loop 对应下边说的猥琐套路
            let currentText = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase();
            match currentText.as_str() {
                global::圆括号_STR => { // 各column名
                    insertValues.useExplicitColumnNames = true;

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnName都要是TextLiteral 而不是StringContent
                        let text = currentElement.expectTextLiteral(global::EMPTY_STR)?;
                        match text.as_str() {
                            global::逗号_STR => continue,
                            // columnName读取结束了 下边应该是values
                            global::圆括号1_STR => break,
                            _ => insertValues.columnNames.push(text),
                        }
                    }

                    // 后边应该到下边的 case "VALUES" 那边 因为rust的match默认有break效果不会到下边的case 需要使用猥琐的套路 把它们都包裹到loop
                }
                "VALUES" => { // values
                    insertValues.columnExprs = self.parseInExprs()?;
                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        // 如果是显式说明的columnName 需要确保columnName数量和value数量相同
        if insertValues.useExplicitColumnNames {
            if insertValues.columnNames.len() != insertValues.columnExprs.len() {
                self.throwSyntaxErrorDetail("column number should equal value number")?;
            }

            if insertValues.columnNames.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column")?;
            }
        } else {
            if insertValues.columnExprs.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column value")?;
            }
        }

        Ok(Command::Insert(insertValues))
    }

    // link user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13)
    // todo 能不能实现 ```link user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre```
    fn parseLink(&mut self, regardLastPartAsFilter: bool) -> Result<Command> {
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
                                exprElementVec = Default::default();
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

    /// unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13) <br>
    /// todo unlink user(id >1 ) as start in usage (number = 7) ,as end in own(number =7) 感觉不该用到unlink上,反而应该用到select上
    fn parseUnlink(&mut self) -> Result<Command> {
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
                                relDesc.relationFliterExpr = Some(self.parseExpr(false)?);
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

    /// delete from user(a=1)
    fn parseDelete(&mut self) -> Result<Command> {
        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("from", "delete should followed by from")?;

        let mut delete = Delete::default();

        delete.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("expect a table after from")?;

        if self.getCurrentElementOption().is_some() {
            delete.filterExpr = Some(self.parseExpr(false)?);
        }

        Ok(Command::Delete(delete))
    }

    /// ```update user[name='a',order=7](id=1)```
    fn parseUpdate(&mut self) -> Result<Command> {
        let mut update = Update::default();

        update.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("update should followed by table name")?;

        // []中的set values
        {
            self.getCurrentElementAdvance()?.expectTextLiteralContent(global::方括号_STR)?;
            enum State {
                ReadName,
                ReadEual,
                ReadExpr,
            }

            let mut state = State::ReadName;
            let mut parserMini = Parser::default();

            let mut columnName = None;

            'outerLoop:
            loop {
                let currentElement = self.getCurrentElementAdvance()?;

                match state {
                    State::ReadName => {
                        columnName.replace(currentElement.expectTextLiteral("expect a column name")?);

                        state = State::ReadEual;
                    }
                    State::ReadEual => {
                        if let Element::Op(Op::MathCmpOp(MathCmpOp::Equal)) = currentElement {
                            state = State::ReadExpr;
                            continue;
                        } else {
                            self.throwSyntaxErrorDetail("column name should followed by equal")?;
                        }
                    }
                    State::ReadExpr => {
                        parserMini.clear();

                        let mut elementVec = Vec::new();

                        macro_rules! getPair {
                            () => {
                                let columnName = columnName.take().unwrap();

                                parserMini.elementVecVec.push(elementVec);
                                let expr = parserMini.parseExpr(false)?;

                                update.columnName_expr.insert(columnName, expr);
                            };
                        }

                        self.skipElement(-1)?;

                        'innerLoop:
                        loop {
                            let currentElement = self.getCurrentElementAdvance()?;

                            if currentElement.expectTextLiteralContentBool(global::逗号_STR) {
                                getPair!();
                                break 'innerLoop;
                            }

                            if currentElement.expectTextLiteralContentBool(global::方括号1_STR) {
                                getPair!();
                                break 'outerLoop;
                            }

                            elementVec.push(currentElement.clone());
                        }

                        state = State::ReadName;
                    }
                }
            }
        }

        // 读取表的过滤expr
        if self.getCurrentElementOption().is_some() {
            update.filterExpr = Some(self.parseExpr(false)?);
        }

        Ok(Command::Update(update))
    }

    // todo 实现 select user(id >1 ) as user0 ,in usage (number = 7) ,end in own(number =7)
    /// ```select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre```
    fn parseSelect(&mut self) -> Result<Command> {
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
                                relDesc.relationFliterExpr = Some(self.parseExpr(false)?);
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

    // todo 能够应对like
    /// 当link sql解析到表名后边的"("时候 调用该函数 不过调用的时候elementIndex还是"("的前边1个 <br>
    /// stopWhenParseRightComplete 用来应对(a>0+6),0+6未被括号保护,不然的话会解析成  (a>1)+6
    fn parseExpr(&mut self, stopWhenParseRightComplete: bool) -> Result<Expr> {
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
                            rightExprVec: Default::default(),
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
                                            rightExprVec: self.parseInExprs()?.into_iter().map(|expr| { Box::new(expr) }).collect(),
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
                                            rightExprVec: vec![Box::new(subExpr)],
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
                            rightExprVec: vec![Box::new(Expr::Single(currentElement))],
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
                                        rightExprVec: vec![Box::new(self.parseExpr(!nextElementIs括号)?)],
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
                                            rightExprVec: vec![Box::new(self.parseExpr(true)?)],
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
                                        rightExprVec: Default::default(),
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
    fn parseInExprs(&mut self) -> Result<Vec<Expr>> {
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
                                    pendingElementVec = Vec::new();
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

    /// 返回None的话说明当前已经是overflow了 和之前遍历char时候不同的是 当不能advance时候index是在最后的index还要向后1个的
    fn getCurrentElementAdvanceOption(&mut self) -> Option<&Element> {
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex);
        if option.is_some() {
            suffix_plus_plus!(self.currentElementIndex);
        }
        option
    }

    /// 得到current element 然后 advance
    fn getCurrentElementAdvance(&mut self) -> Result<&Element> {
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex);
        if option.is_some() {
            suffix_plus_plus!(self.currentElementIndex);
            Ok(option.unwrap())
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    fn getCurrentElement(&self) -> Result<&Element> {
        let option = self.getCurrentElementOption();
        if option.is_some() {
            Ok(option.unwrap())
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    fn getCurrentElementOption(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex)
    }

    /// 和 peekNextElement 不同的是 得到的是 Option 不是 result
    fn peekPrevElementOpt(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex - 1)
    }

    fn peekPrevElement(&self) -> Result<&Element> {
        if let Some(previousElement) = self.peekPrevElementOpt() {
            Ok(previousElement)
        } else {
            self.throwSyntaxError()?
        }
    }

    fn peekNextElementOpt(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex + 1)
    }

    fn peekNextElement(&self) -> Result<&Element> {
        if let Some(nextElement) = self.peekNextElementOpt() {
            Ok(nextElement)
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    /// 和parse toke 遍历char不同的是 要是越界了 index会是边界的后边1个 以符合当前的体系
    fn skipElement(&mut self, delta: i32) -> Result<()> {
        let currentElementVecLen = self.elementVecVec.get(self.currentElementVecIndex).unwrap().len();

        if (self.currentElementIndex as i32 + delta) as usize >= currentElementVecLen {
            self.currentElementIndex = currentElementVecLen;
            self.throwSyntaxError()
        } else {
            self.currentElementIndex = (self.currentElementIndex as i32 + delta) as usize;
            Ok(())
        }
    }

    /// 字母数字 且 数字不能打头
    fn checkDbObjectName(&self, name: &str) -> Result<()> {
        let chars: Vec<char> = name.chars().collect();

        // 打头得要字母
        match chars[0] {
            'a'..='z' => {}
            'A'..='Z' => {}
            _ => self.throwSyntaxErrorDetail("table,column name should start with letter")?,
        }

        if name.len() == 1 {
            return Ok(());
        }

        for char in chars[1..].iter() {
            match char {
                'a'..='z' => {}
                'A'..='Z' => {}
                '0'..='9' => {}
                _ => self.throwSyntaxErrorDetail("table,column name should only contain letter , number")?,
            }
        }

        Ok(())
    }

    fn resetCurrentElementIndex(&mut self) {
        self.currentElementIndex = 0;
    }
}

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
    To,
    /// parse时候用不到的 link用到
    PointDesc(PointDesc),
    Null,
}

impl Element {
    fn expectTextLiteralOpt(&self) -> Option<String> {
        if let Element::TextLiteral(text) = self {
            Some(text.to_string())
        } else {
            None
        }
    }

    fn expectTextLiteral(&self, errorStr: &str) -> Result<String> {
        if let Some(text) = self.expectTextLiteralOpt() {
            Ok(text.to_string())
        } else {
            throw!(&format!("expect Element::TextLiteral but get {:?}, {}", self, errorStr))
        }
    }

    fn expectTextLiteralContent(&self, expectContent: &str) -> Result<()> {
        if self.expectTextLiteralContentBool(expectContent) {
            Ok(())
        } else {
            throw!(&format!("expect Element::TextLiteral({}) but get {:?}", expectContent, self))
        }
    }

    fn expectTextLiteralContentBool(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            content == expectContent
        } else {
            false
        }
    }

    fn expectTextLiteralContentIgnoreCaseBool(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            let expectContent = expectContent.to_uppercase();
            let content = content.to_uppercase();

            expectContent == content
        } else {
            false
        }
    }

    fn expectTextLiteralContentIgnoreCase(&self, expectContent: &str, errorStr: &str) -> Result<()> {
        if self.expectTextLiteralContentIgnoreCaseBool(expectContent) {
            Ok(())
        } else {
            throw!(errorStr)
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
            Element::To => write!(f, "To"),
            Element::Null => write!(f, "Null"),
            _ => write!(f, "unknown")
        }
    }
}

impl Debug for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Insert {
    pub tableName: String,
    /// insert into table (column) values ('a')
    pub useExplicitColumnNames: bool,
    pub columnNames: Vec<String>,
    pub columnExprs: Vec<Expr>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Op {
    MathCmpOp(MathCmpOp),
    SqlOp(SqlOp),
    LogicalOp(LogicalOp),
    MathCalcOp(MathCalcOp),
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::MathCmpOp(s) => write!(f, "MathCmpOp({})", s),
            Op::LogicalOp(s) => write!(f, "LogicalOp({})", s),
            Op::SqlOp(s) => write!(f, "SqlOp({})", s),
            Op::MathCalcOp(mathCalcOp) => write!(f, "MathCalcOp({})", mathCalcOp),
        }
    }
}

// https://note.qidong.name/2023/03/rust-enum-str/
#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum MathCmpOp {
    Equal,
    GreaterThan,
    GreaterEqual,
    LessEqual,
    LessThan,
    NotEqual,
}

/// "a".parse::<MathCmpOp>()用的
impl FromStr for MathCmpOp {
    type Err = GraphError;

    fn from_str(str: &str) -> std::result::Result<Self, Self::Err> {
        match str {
            global::等号_STR => Ok(MathCmpOp::Equal),
            global::小于_STR => Ok(MathCmpOp::LessThan),
            global::大于_STR => Ok(MathCmpOp::GreaterThan),
            global::小于等于_STR => Ok(MathCmpOp::LessEqual),
            global::大于等于_STR => Ok(MathCmpOp::GreaterEqual),
            global::不等_STR => Ok(MathCmpOp::NotEqual),
            _ => throw!(&format!("unknown math cmp op :{}",str)),
        }
    }
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum SqlOp {
    In,
}

#[derive(DisplayStrum, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum MathCalcOp {
    Plus,
    Divide,
    Multiply,
    Minus,
}

impl MathCalcOp {
    pub fn fromChar(char: char) -> Result<Self> {
        match char {
            global::加号_CHAR => Ok(MathCalcOp::Plus),
            global::除号_CHAR => Ok(MathCalcOp::Divide),
            global::乘号_CHAR => Ok(MathCalcOp::Multiply),
            global::减号_CHAR => Ok(MathCalcOp::Minus),
            _ => throw!(&format!("unknown math calc operator:{char}"))
        }
    }
}

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

#[derive(Debug, Serialize, Deserialize)]
pub enum Select {
    SelectTable(SelectTable),
    SelectRels(Vec<SelectRel>),
    SelectTableUnderRels(SelectTableUnderRels),
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Delete {
    pub tableName: String,
    pub filterExpr: Option<Expr>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Update {
    pub tableName: String,
    // todo insert的values的expr要能支持含column name的
    pub columnName_expr: HashMap<String, Expr>,
    pub filterExpr: Option<Expr>,
}

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

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct RelDesc {
    pub endPointType: EndPointType,
    pub relationName: String,
    pub relationFliterExpr: Option<Expr>,
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

// ------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use crate::parser;
    use crate::parser::Parser;

    #[test]
    pub fn testParseCreateTable() {
        parser::parse("CREATE    TABLE    TEST   ( COLUMN1 string null  ,  COLUMN2 DECIMAL null)").unwrap();
    }

    #[test]
    pub fn testParseInsert() {
        // println!("{}", "".parse::<f64>().unwrap());
        parser::parse("insert into user values (1,null)").unwrap();
    }

    #[test]
    pub fn testParseSelect() {
        // parser::parse("select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> wheel").unwrap();
        parser::parse("select user(id >1 ) as user0 ,in usage (number = 7) ,as end in own(number =7)").unwrap();
    }

    #[test]
    pub fn testParseLink() {
        // parser::parse("link user(id > 1 and ( name = 'a' or code = (1 + 0) and true))").unwrap();
        // parser::parse("link user (a>0+6+1>a)").unwrap();
        // parser::parse("link user (a in (0,0+6,0+(a+1),))").unwrap();
        // parser::parse(" a = 1+0 and b='a'").unwrap();
        // parser::parse("link user ((a = 1) = true)").unwrap();
        // parser::parse("link user (((a = 1)) = true)").unwrap();
        // parser::parse("link user ( a in (a,b,d))").unwrap();
        parser::parse("link user ( a in ((a = 1) = true)) to company (id > 1 and ( name = 'a' or code = 1 + 0 and true)) by usage(a=0,a=1212+0,d=1)").unwrap();
    }

    #[test]
    pub fn testUpdate() {
        parser::parse("update user[name='a',order=7]").unwrap();
    }

    #[test]
    pub fn testParseDelete() {
        parser::parse("delete from user(a=0)").unwrap();
    }

    #[test]
    pub fn testUnlink() {
        //parser::parse("unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13)").unwrap();
        parser::parse("unlink user(id >1 ) as start by usage (number = 7) ,as end by own(number =7)").unwrap();
    }

    #[test]
    pub fn testChinese() {
        let chinese = r#"   秀 a"#;
        let mut chars = chinese.chars();
        println!("{}", chars.next().unwrap() == ' ');
    }

    #[test]
    pub fn testMultiLines() {
        let sql = "create table if not exist user (id integer,name string)
                       insert into user values (1,'tom')";

        parser::parse(sql).unwrap();
    }
}


