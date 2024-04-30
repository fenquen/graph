use std::cmp::PartialEq;
use std::fmt::{Debug, Display, Formatter, Pointer};
use std::str::FromStr;
use crate::{global, prefix_plus_plus, suffix_minus_minus, suffix_plus_plus, throw};
use anyhow::Result;
use lazy_static::lazy_static;
use strum_macros::{Display as DisplayStrum, Display, EnumString};
use crate::graph_error::GraphError;
use crate::meta::{Column, ColumnType, Table, TableType, ColumnValue};

pub fn parse(sql: &str) -> Result<Vec<Command>> {
    let mut parser = Parser::new(sql);

    parser.parseElement()?;

    for elementVec in &parser.elementVecVec {
        for element in elementVec {
            println!("{}", element);
        }

        println!("{}", "\n");
    }

    parser.parse()
    //Ok(Default::default())
}

pub enum Command {
    CreateTable(Table),
    Insert(InsertValues),
    Link(Link),
    SELECT,
    UNKNOWN,
}

#[derive(Default)]
pub struct Parser {
    sql: String,

    chars: Vec<char>,
    currentCharIndex: usize,
    pendingChars: Vec<char>,
    单引号as文本边界的数量: usize,
    括号数量: usize,
    括号1数量: usize,

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

    fn parseElement(&mut self) -> Result<()> {
        let mut currentElementVec: Vec<Element> = Vec::new();

        // 空格 逗号 单引号 括号
        loop {
            let mut advanceCount: usize = 1;

            // "insert   INTO TEST VALUES ( ',',1 )"
            let currentChar = self.currentChar();
            match currentChar {
                // 空格如果不是文本内容的话不用记录抛弃
                global::空格_CHAR => {
                    // 是不是文本本身的内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        self.collectPendingChars(&mut currentElementVec);
                    }
                }
                global::单引号_CHAR => {
                    if self.whetherIn单引号() {
                        match self.nextChar() {
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
                global::括号_CHAR | global::括号1_CHAR | global::逗号_CHAR => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        self.collectPendingChars(&mut currentElementVec);

                        // 本身也添加到elementVec
                        currentElementVec.push(Element::TextLiteral(currentChar.to_string()));

                        if currentChar == global::括号_CHAR {
                            self.括号数量 = self.括号数量 + 1;
                        } else if currentChar == global::括号1_CHAR {
                            self.括号1数量 = self.括号1数量 + 1;
                        }
                    }
                }
                global::分号_CHAR => { // 应对同时写了多个以;分隔的sql
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
                            if let Some(nextChar) = self.nextChar() {
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

                        let mathCmpOp = operatorString.as_str().parse()?;

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);

                        currentElementVec.push(Element::Op(Op::MathCmpOp(mathCmpOp)));
                    }
                }
                // 数学计算符 因为是可以粘连的 需要到这边来parse
                '+' | '/' | '*' | '-' => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        let mathCalcOp = MathCalcOp::from(currentChar);
                        if let MathCalcOp::Unknown = mathCalcOp {
                            self.throwSyntaxErrorDetail(&format!("unknown math calc operator:{}", currentChar))?;
                        }

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);

                        currentElementVec.push(Element::Op(Op::MathCalcOp(mathCalcOp)));
                    }
                }
                _ => self.pendingChars.push(currentChar),
            }

            let reachEnd = self.advanceChar(advanceCount);
            if reachEnd {
                self.collectPendingChars(&mut currentElementVec);
                self.elementVecVec.push(currentElementVec);
                break;
            }
        }

        // 需要确保单引号 和括号是对称的
        if self.whetherIn单引号() || self.括号数量 != self.括号1数量 {
            self.throwSyntaxError()?;
        }

        if self.elementVecVec.len() == 0 {
            self.throwSyntaxErrorDetail("the sql is empty string")?;
        }

        Ok(())
    }

    /// 要是会已到末尾以外 返回true
    fn advanceChar(&mut self, count: usize) -> bool {
        if self.currentCharIndex + count >= self.sql.len() {
            self.currentCharIndex = self.sql.len() - 1;
            return true;
            // throw!("当前已是sql的末尾不能advance了");
        }

        self.currentCharIndex = self.currentCharIndex + count;

        false
    }

    fn currentChar(&self) -> char {
        self.chars[self.currentCharIndex]
    }

    fn previousChar(&self) -> Option<char> {
        if self.currentCharIndex == 0 {
            None
        } else {
            Some(self.chars[self.currentCharIndex - 1])
        }
    }

    /// peek而已不会变化currentCharIndex
    fn nextChar(&self) -> Option<char> {
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
                        "IS" => Element::Op(Op::SqlOp(SqlOp::Is)),
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
                "LINK" => self.parseLink()?,
                _ => self.throwSyntaxError()?,
            };

            commandVec.push(command);

            if prefix_plus_plus!(self.currentElementVecIndex) >= self.elementVecVec.len() {
                break;
            }

            self.currentElementIndex = 0;
        }

        Ok(commandVec)
    }

    /// 当前不实现 default value
    // CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)
    fn parseCreate(&mut self) -> Result<Command> {
        // 不是table便是relation
        let tableType = self.getCurrentElement()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase().as_str().parse()?;
        self.parseCreateTable(tableType)
    }

    fn parseCreateTable(&mut self, tableType: TableType) -> Result<Command> {
        let mut table = Table::default();

        table.type0 = tableType;

        // 读取table name
        table.name = self.getCurrentElementAdvance()?.expectTextLiteral("table name can not be pure number")?;

        self.checkDbObjectName(&table.name)?;

        // 应该是"("
        self.getCurrentElementAdvance()?.expectTextLiteralContent(global::括号_STR)?;

        // 循环读取 column
        enum ReadColumnState {
            ReadColumnName,
            ReadColumnType,
            ReadComplete,
        }

        let mut readColumnState = ReadColumnState::ReadColumnName;
        let mut column = Column::default();
        loop {
            let element = self.getCurrentElementOptionAdvance();
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
                            let columnType = ColumnType::from(text.to_uppercase().as_str());
                            match columnType {
                                ColumnType::UNKNOWN => self.throwSyntaxErrorDetail(&format!("unknown column type:{}", text))?,
                                _ => column.type0 = columnType,
                            }

                            readColumnState = ReadColumnState::ReadComplete;
                        }
                        ReadColumnState::ReadComplete => {
                            match text.as_str() {
                                global::逗号_STR => {
                                    readColumnState = ReadColumnState::ReadColumnName;

                                    table.columns.push(column);
                                    column = Column::default();

                                    continue;
                                }
                                global::括号1_STR => {
                                    table.columns.push(column);
                                    break;
                                }
                                _ => self.throwSyntaxError()?,
                            }
                        }
                    }
                }
                _ => self.throwSyntaxErrorDetail("column name,column type can not be pure number")?,
            }
        }

        Ok(Command::CreateTable(table))
    }

    // insert   INTO TEST VALUES ( '0'  , ')')
    // insert into test (column1) values ('a')
    // a
    fn parseInsert(&mut self) -> Result<Command> {
        let currentElement = self.getCurrentElementAdvance()?;
        if currentElement.expectTextLiteralContentIgnoreCaseBool("into") == false {
            self.throwSyntaxErrorDetail("insert should followed by into")?;
        }

        let mut insertValues = InsertValues::default();

        insertValues.tableName = self.getCurrentElementAdvance()?.expectTextLiteral("table name should not pure number")?.to_string();

        loop { // loop 对应下边说的猥琐套路
            let currentText = self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase();
            match currentText.as_str() {
                "(" => { // 各column名
                    insertValues.useExplicitColumnNames = true;

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnName都要是TextLiteral 而不是StringContent
                        let text = currentElement.expectTextLiteral(global::EMPTY_STR)?;
                        match text.as_str() {
                            global::逗号_STR => continue,
                            // columnName读取结束了 下边应该是values
                            ")" => break,
                            _ => insertValues.columnNames.push(text),
                        }
                    }

                    // 后边应该到下边的 case "VALUES" 那边 因为rust的match默认有break效果不会到下边的case 需要使用猥琐的套路 把它们都包裹到loop
                }
                "VALUES" => { // values
                    self.getCurrentElementAdvance()?.expectTextLiteralContent(global::括号_STR)?;

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnValue 不能是TextLiteral
                        match currentElement {
                            Element::StringContent(stringContent) => {
                                insertValues.columnValues.push(ColumnValue::STRING(stringContent.to_string()));
                            }
                            Element::IntegerLiteral(int) => {
                                insertValues.columnValues.push(ColumnValue::INTEGER(*int));
                            }
                            Element::DecimalLiteral(decimal) => {
                                insertValues.columnValues.push(ColumnValue::DECIMAL(*decimal));
                            }
                            Element::TextLiteral(text) => {
                                match text.as_str() {
                                    global::逗号_STR => continue,
                                    global::括号1_STR => break,
                                    _ => self.throwSyntaxErrorDetail("column value should not be text literal")?,
                                }
                            }
                            _ => {}
                        }
                    }

                    break;
                }
                _ => {
                    self.throwSyntaxError()?;
                }
            }
        }

        // 如果是显式说明的columnName 需要确保columnName数量和value数量相同
        if insertValues.useExplicitColumnNames {
            if insertValues.columnNames.len() != insertValues.columnValues.len() {
                self.throwSyntaxErrorDetail("column number should equal value number")?;
            }

            if insertValues.columnNames.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column")?;
            }
        } else {
            if insertValues.columnValues.len() == 0 {
                self.throwSyntaxErrorDetail("you have not designate any column value")?;
            }
        }

        Ok(Command::Insert(insertValues))
    }

    // link user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13)
    fn parseLink(&mut self) -> Result<Command> {
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
            match (parseSrcDestState, self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_uppercase().as_str()) {
                (ParseSrcDestState::ParseSrcTableName, tableName) => {
                    link.srcTableName = tableName.to_string();
                    parseSrcDestState = ParseSrcDestState::ParseSrcTableCondition;
                }
                (ParseSrcDestState::ParseSrcTableCondition, global::括号_STR) => {
                    // 返回1个确保当前的element是"("
                    self.skipElement(-1)?;
                    link.srcTableCondition = Some(self.parseExpr(false)?);

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
                (ParseSrcDestState::ParseDestTableCondition, global::括号_STR) => {
                    self.skipElement(-1)?;
                    link.destTableCondition = Some(self.parseExpr(false)?);
                    break;
                }
                _ => self.throwSyntaxError()?,
            }
        }

        self.getCurrentElementAdvance()?.expectTextLiteralContentIgnoreCase("by", "missing 'by'")?;

        link.relationName = self.getCurrentElementAdvance()?.expectTextLiteral("relation name")?;

        // 下边要解析 by usage (a=0,a=(1212+0))的后边的括号部分了
        if let Some(element) = self.getCurrentElementOptionAdvance() {
            element.expectTextLiteralContent(global::括号_STR)?;
        } else { // 未写link的value
            return Ok(Command::Link(link));
        }

        #[derive(Clone, Copy)]
        enum ParseState {
            ParseColumnName,
            ParseEqual,
            ParseColumnExpr,
            AfterParseColumnExpr,
        }

        let mut parseState = ParseState::ParseColumnName;

        // 和parseInExprs使用相同的套路,当(数量和)数量相同的时候说明结束了
        let mut 括号数量 = 0;
        let mut 括号1数量 = 0;

        // 会以")"收尾
        let mut exprElementVec = Default::default();
        loop {
            break;
            let currentElement = self.getCurrentElementAdvance()?;

            match (parseState, currentElement) {
                (ParseState::ParseColumnName, Element::TextLiteral(columnName)) => {
                    link.columnNames.push(columnName.to_string());
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
                        link.columnValues.push(parser.parseExpr(false)?);
                        exprElementVec = Default::default();

                        // 到下轮的parseColumnName
                        parseState = ParseState::ParseColumnName;
                        continue;
                    }

                    if currentElement.expectTextLiteralContentBool(global::括号_STR) {
                        括号数量 = 括号数量 + 1;
                    } else if currentElement.expectTextLiteralContentBool(global::括号1_STR) {
                        括号1数量 = 括号1数量 + 1;

                        // 说明到了last的)
                        if 括号数量 == 括号1数量 {
                            let mut parser = Parser::default();
                            parser.elementVecVec.push(exprElementVec);
                            link.columnValues.push(parser.parseExpr(false)?);
                            exprElementVec = Default::default();
                            break;
                        }
                    }

                    exprElementVec.push(currentElement.clone());
                }
                _ => self.throwSyntaxError()?,
            }
        }

        println!("{:?}", link);
        Ok(Command::Link(link))
    }

    /// 当link sql解析到表名后边的"("时候 调用该函数 不过调用的时候elementIndex还是"("的前边1个 <br>
    /// stopWhenParseRightComplete 用来应对(a>0+6),0+6未被括号保护,不然的话会解析成  (a>1)+6
    fn parseExpr(&mut self, stopWhenParseRightComplete: bool) -> Result<Expr> {
        let mut hasLeading括号 = false;

        if self.getCurrentElement()?.expectTextLiteralContentBool(global::括号_STR) {
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
            let currentElement = match self.getCurrentElementOptionAdvance() {
                None => break,
                Some(currentElement) => currentElement.clone(),
            };

            match parseCondState {
                ParseCondState::ParsingLeft => {
                    if currentElement.expectTextLiteralContentBool(global::括号_STR) {
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
                            left: Box::new(expr),
                            op,
                            right: Default::default(),
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
                            if text == global::括号_STR {
                                // 要应对 a in ('a'),那么碰到"("的话需要去看看前边的是不是 in

                                // 需要先回过去然后回过来,不然prevElement还是currentElement
                                self.skipElement(-1)?;
                                let previousElement = self.peekPrevElement()?.clone();
                                self.skipElement(1)?;

                                // 说明是 "... in ( ..." 这样的,括号对应的便不是单个expr而是多个expr
                                if let Element::Op(Op::SqlOp(SqlOp::In)) = previousElement {
                                    self.skipElement(-1)?;

                                    // 得要BiDirection
                                    if let Expr::BiDirection { left, op, .. } = expr {
                                        expr = Expr::BiDirection {
                                            left,
                                            op,
                                            right: self.parseInExprs()?.into_iter().map(|expr| { Box::new(expr) }).collect(),
                                        }
                                    } else {
                                        self.throwSyntaxError()?;
                                    }
                                } else if let Element::Op(_) = previousElement { // 前边是别的op
                                    self.skipElement(-1)?;

                                    // 递归
                                    let subExpr = self.parseExpr(false)?;

                                    // 得要BiDirection
                                    if let Expr::BiDirection { left, op, .. } = expr {
                                        expr = Expr::BiDirection {
                                            left,
                                            op,
                                            right: vec![Box::new(subExpr)],
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

                    if let Expr::BiDirection { left, op, .. } = expr {
                        expr = Expr::BiDirection {
                            left,
                            op,
                            right: vec![Box::new(Expr::Single(currentElement))],
                        }
                    } else {
                        self.throwSyntaxError()?;
                    }

                    parseCondState = ParseCondState::ParseRightComplete;
                }
                ParseCondState::ParseRightComplete => {
                    if stopWhenParseRightComplete {
                        // 不要遗忘
                        self.skipElement(-1)?;
                        break;
                    }

                    match currentElement {
                        Element::TextLiteral(text) => {
                            if hasLeading括号 == false {
                                self.skipElement(-1)?;
                                break;
                            }

                            // (a = 1) 的 ")",说明要收了，递归结束要返回上轮
                            if text == global::括号1_STR {
                                break;
                            }
                        }
                        Element::Op(op) => {
                            // 需要区分 原来是都是认为是logicalOp
                            match op {
                                // 它是之前能应对的情况 a = 1 and b= 0 的 and
                                // (a and b or d) 会解析变成 a and (b or d) 不对 应该是 (a and b) or d
                                Op::LogicalOp(_) => {
                                    let a = self.getCurrentElement()?.expectTextLiteralContentBool(global::括号_STR);

                                    expr = Expr::BiDirection {
                                        left: Box::new(expr),
                                        op,
                                        // 需要递归下钻
                                        right: vec![Box::new(self.parseExpr(!a)?)],
                                    };
                                    // (m and (a = 0 and (b = 1))) 这个时候解析到的是1后边的那个")"而已 还有")"残留
                                    // (a=0 and (b=1) and 1 or 0)
                                    parseCondState = ParseCondState::ParseRightComplete;
                                    continue;
                                }
                                // a>0+6 and b=0 的 "+",当前的expr是a>0,需要打破现有的expr
                                Op::MathCalcOp(_) => {
                                    if let Expr::BiDirection { left, op, .. } = expr {
                                        // 需要先回到0+6的起始index
                                        self.skipElement(-2)?;

                                        expr = Expr::BiDirection {
                                            left,
                                            op,
                                            // 递归的level不能用力太猛 不然应对不了 a > 0+6 and b=0 会把 0+6 and b=0 当成1个expr
                                            right: vec![Box::new(self.parseExpr(true)?)],
                                        };

                                        parseCondState = ParseCondState::ParseRightComplete;
                                        continue;
                                    } else {
                                        self.throwSyntaxError()?;
                                    }
                                }
                                // 0+6>a and b=0的 ">" 当前的expr是0+6
                                Op::MathCmpOp(_) => {
                                    // 把现有的expr降级变为小弟
                                    expr = Expr::BiDirection {
                                        left: Box::new(expr),
                                        op,
                                        right: Default::default(),
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
        self.getCurrentElement()?.expectTextLiteralContent(global::括号_STR)?;

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
                        global::括号_STR => {
                            pendingElementVec.push(currentElement.clone());

                            suffix_plus_plus!(括号count);
                        }
                        // 要以)收尾
                        global::括号1_STR => {
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
    fn getCurrentElementOptionAdvance(&mut self) -> Option<&Element> {
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
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex);
        if option.is_some() {
            Ok(option.unwrap())
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
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

        if (self.currentElementIndex as i32 + delta) as usize >= self.elementVecVec.get(self.currentElementVecIndex).unwrap().len() {
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
}

#[derive(Clone)]
pub enum Element {
    /// 如果只有TextLiteral的话 还是不能区分 (')') 的两个右括号的
    TextLiteral(String),
    /// 对应''包括起来的内容
    StringContent(String),
    IntegerLiteral(i64),
    DecimalLiteral(f64),
    Op(Op),
    Boolean(bool),
    Unknown,
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

impl Default for Element {
    fn default() -> Self {
        Element::Unknown
    }
}

impl Display for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Element::TextLiteral(s) => write!(f, "{}({})", "TextLiteral", s),
            Element::StringContent(s) => write!(f, "{}({})", "StringContent", s),
            Element::IntegerLiteral(s) => write!(f, "{}({})", "IntegerLiteral", s),
            Element::DecimalLiteral(s) => write!(f, "{}({})", "DecimalLiteral", s),
            Element::Boolean(bool) => write!(f, "{}({})", "Boolean", bool),
            Element::Op(op) => write!(f, "{}({})", "Op", op),
            _ => write!(f, "{}", "Unknown"),
        }
    }
}

impl Debug for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Default)]
pub struct InsertValues {
    pub tableName: String,
    /// insert into table (column) values ('a')
    pub useExplicitColumnNames: bool,
    pub columnNames: Vec<String>,
    pub columnValues: Vec<ColumnValue>,
}

#[derive(Clone, Debug)]
pub enum Op {
    MathCmpOp(MathCmpOp),
    SqlOp(SqlOp),
    LogicalOp(LogicalOp),
    Unknown,
    MathCalcOp(MathCalcOp),
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::MathCmpOp(s) => write!(f, "MathCmpOp({})", s),
            Op::LogicalOp(s) => write!(f, "LogicalOp({})", s),
            Op::SqlOp(s) => write!(f, "SqlOp({})", s),
            Op::MathCalcOp(mathCalcOp) => write!(f, "MathCalcOp({})", mathCalcOp),
            _ => write!(f, "Unknown"),
        }
    }
}

impl Default for Op {
    fn default() -> Self {
        Op::Unknown
    }
}

// https://note.qidong.name/2023/03/rust-enum-str/
#[derive(DisplayStrum, Clone, Debug)]
pub enum MathCmpOp {
    Equal,
    GreaterThan,
    GreaterEqual,
    LessEqual,
    LessThan,
    NotEqual,
    Unknown,
}

impl From<&str> for MathCmpOp {
    fn from(str: &str) -> Self {
        match str {
            global::等号_STR => MathCmpOp::Equal,
            global::小于_STR => MathCmpOp::LessThan,
            global::大于_STR => MathCmpOp::GreaterThan,
            global::小于等于_STR => MathCmpOp::LessEqual,
            global::大于等于_STR => MathCmpOp::GreaterEqual,
            global::不等_STR => MathCmpOp::NotEqual,
            _ => MathCmpOp::Unknown,
        }
    }
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

#[derive(DisplayStrum, Clone, Debug)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(DisplayStrum, Clone, Debug)]
pub enum SqlOp {
    In,
    Is,
}

#[derive(DisplayStrum, Clone, Debug)]
pub enum MathCalcOp {
    Plus,
    Divide,
    Multiply,
    Minus,
    Unknown,
}

impl FromStr for MathCalcOp {
    type Err = GraphError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "+" => Ok(MathCalcOp::Plus),
            "/" => Ok(MathCalcOp::Divide),
            "*" => Ok(MathCalcOp::Multiply),
            "-" => Ok(MathCalcOp::Minus),
            _ => throw!(&format!("unknown math compare operator:{}",s))
        }
    }
}

impl From<char> for MathCalcOp {
    fn from(char: char) -> Self {
        match char {
            '+' => MathCalcOp::Plus,
            '/' => MathCalcOp::Divide,
            '*' => MathCalcOp::Multiply,
            '-' => MathCalcOp::Minus,
            _ => MathCalcOp::Unknown,
        }
    }
}

/// link user(id = 1) to car(color = 'red') by usage(number = 2)
#[derive(Default, Debug)]
pub struct Link {
    pub srcTableName: String,
    pub srcTableCondition: Option<Expr>,

    pub destTableName: String,
    pub destTableCondition: Option<Expr>,

    pub relationName: String,
    pub columnNames: Vec<String>,
    pub columnValues: Vec<Expr>,
}

// 碰到"(" 下钻递归,返回后落地到上级的left right
#[derive(Debug)]
pub enum Expr {
    Single(Element),
    BiDirection {
        left: Box<Expr>,
        op: Op,
        right: Vec<Box<Expr>>,
    },
    None,
}

impl Default for Expr {
    fn default() -> Self {
        Expr::None
    }
}

// ------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use crate::parser;
    use crate::parser::Parser;

    #[test]
    pub fn testParseCreateTable() {
        parser::parse("CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)").unwrap();
    }

    #[test]
    pub fn testParseInsert() {
        // println!("{}", "".parse::<f64>().unwrap());
        parser::parse("insert   INTO TEST VALUES ( 0  , ')')").unwrap();
    }

    #[test]
    pub fn testLink() {
        // "link user(id > 1 and ( name = 'a' or code = 6)) to car (color='red') by usage(number = 13)"
        // parser::parse("link user(id > 1 and ( name = 'a' or code = (1 + 0) and true))").unwrap();
        // parser::parse("link user (a>0+6+1>a)").unwrap();
        // parser::parse("link user (a in (0,0+6,0+(a+1),))").unwrap();
        // parser::parse(" a = 1+0 and b='a'").unwrap();
        // parser::parse("link user ((a = 1) = true)").unwrap();
        // parser::parse("link user (((a = 1)) = true)").unwrap();
        // parser::parse("link user ( a in (a,b,d))").unwrap();
        parser::parse("link user ( a in ((a = 1) = true)) to company (id > 1 and ( name = 'a' or code = 1 + 0 and true)) by usage(a=0,a=1212+0,d=1)").unwrap();
    }
}


