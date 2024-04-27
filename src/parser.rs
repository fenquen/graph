use std::cmp::PartialEq;
use std::fmt::{Debug, Display, Formatter, Pointer};
use crate::{global, prefix_plus_plus, suffix_minus_minus, suffix_plus_plus, throw};
use anyhow::Result;
use lazy_static::lazy_static;
use strum_macros::{Display as DisplayStrum, EnumString};
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
                global::分号_char => { // 应对同时写了多个以;分隔的sql
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
                // 要解析数学符了
                global::等号_char | global::小于_char | global::大于_char | global::感叹_char => {
                    // 单纯currentCharIndex的是文本内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        let operatorString: String =
                            // 应对  "!=" ">=" "<=" 两个char的 目前的不容许有空格的
                            if let Some(nextChar) = self.nextChar() {
                                match nextChar {
                                    global::等号_char | global::小于_char | global::大于_char | global::感叹_char => {
                                        advanceCount = 2;
                                        vec![currentChar, nextChar].iter().collect()
                                    }
                                    _ => { // 还是1元的operator
                                        vec![currentChar].iter().collect()
                                    }
                                }
                            } else {
                                vec![currentChar].iter().collect()
                            };

                        let mathCmpOp = MathCmpOp::from(operatorString.as_str());

                        if let MathCmpOp::Unknown = mathCmpOp {
                            self.throwSyntaxErrorDetail(&format!("unknown operator:{}", operatorString))?;
                        }

                        // 需要了断 pendingChars
                        self.collectPendingChars(&mut currentElementVec);

                        currentElementVec.push(Element::Op(Op::MathCmpOp(mathCmpOp)));
                    }
                }
                _ => {
                    self.pendingChars.push(currentChar);
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
        if 0 > self.currentCharIndex - 1 {
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
                    // parse 要求不能是大小写混合的
                    match text.to_uppercase().as_str() {
                        "FALSE" => { Element::Boolean(false) }
                        "TRUE" => { Element::Boolean(true) }
                        "OR" => { Element::Op(Op::LogicalOp(LogicalOp::Or)) }
                        "AND" => { Element::Op(Op::LogicalOp(LogicalOp::And)) }
                        "IS" => { Element::Op(Op::SqlOp(SqlOp::Is)) }
                        "IN" => { Element::Op(Op::SqlOp(SqlOp::In)) }
                        _ => { Element::TextLiteral(text) }
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
                '0'..='9' => {
                    continue;
                }
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
                _ => {
                    return (false, false);
                }
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
            let command = match self.getCurrentElementAdvance()? {
                Element::TextLiteral(text) => {
                    let text = text.to_uppercase();

                    match text.as_str() {
                        "CREATE" => {
                            self.parseCreate()?
                        }
                        "INSERT" => {
                            self.parseInsert()?
                        }
                        "LINK" => {
                            self.parseLink()?
                        }
                        _ => {
                            self.throwSyntaxError()?
                        }
                    }
                }
                _ => self.throwSyntaxError()?
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
        let element = self.getCurrentElementAdvance()?;

        // 不是table便是relation
        if let Element::TextLiteral(text) = element {
            let text = text.to_uppercase();
            let text = text.as_str();
            match text {
                "TABLE" | "RELATION" => {
                    let tableType = TableType::from(text);

                    match tableType {
                        TableType::UNKNOWN => {
                            self.throwSyntaxError()
                        }
                        _ => {
                            self.parseCreateTable(tableType)
                        }
                    }
                }
                _ => {
                    self.throwSyntaxError()
                }
            }
        } else {
            self.throwSyntaxError()
        }
    }

    fn parseCreateTable(&mut self, tableType: TableType) -> Result<Command> {
        let mut table = Table::default();

        table.type0 = tableType;

        // 读取table name
        match self.getCurrentElementAdvance()? {
            Element::TextLiteral(tableName) => {
                table.name = tableName.to_string();
            }
            _ => { // 表名不能是纯数字的
                self.throwSyntaxErrorDetail("table name can not be pure number")?;
            }
        }

        self.checkDbObjectName(&table.name)?;

        // 应该是"("
        let element = self.getCurrentElementAdvance()?;
        if element.expectTextLiteralContent("(") == false {
            self.throwSyntaxError()?;
        }

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
                                ColumnType::UNKNOWN => {
                                    self.throwSyntaxErrorDetail(&format!("unknown column type:{}", text))?;
                                }
                                _ => {
                                    column.type0 = columnType;
                                }
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
                                ")" => {
                                    table.columns.push(column);
                                    break;
                                }
                                _ => {
                                    self.throwSyntaxError()?;
                                }
                            }
                        }
                    }
                }
                _ => {
                    self.throwSyntaxErrorDetail("column name,column type can not be pure number")?;
                }
            }
        }

        Ok(Command::CreateTable(table))
    }

    // insert   INTO TEST VALUES ( '0'  , ')')
    // insert into test (column1) values ('a')
    // a
    fn parseInsert(&mut self) -> Result<Command> {
        let currentElement = self.getCurrentElementAdvance()?;
        if currentElement.expectTextLiteralContentIgnoreCase("into") == false {
            self.throwSyntaxErrorDetail("insert should followed by into")?;
        }

        let mut insertValues = InsertValues::default();

        let tableNameElement = self.getCurrentElementAdvance()?;
        if let Element::TextLiteral(tableName) = tableNameElement {
            insertValues.tableName = tableName.to_string();
        } else {
            self.throwSyntaxErrorDetail("table name should not pure number")?;
        }

        loop { // loop 对应下边说的猥琐套路
            let currentElement = self.getCurrentElementAdvance()?;
            let currentText = currentElement.expectTextLiteral().map_or_else(|| { self.throwSyntaxError() }, |s| { Ok(s) })?.to_uppercase();
            match currentText.as_str() {
                "(" => { // 各column名
                    insertValues.useExplicitColumnNames = true;

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnName都要是TextLiteral 而不是StringContent
                        match currentElement.expectTextLiteral() {
                            Some(text) => {
                                match text.as_str() {
                                    global::逗号_STR => {
                                        continue;
                                    }
                                    ")" => { // columnName读取结束了 下边应该是values
                                        break;
                                    }
                                    _ => {
                                        insertValues.columnNames.push(text);
                                    }
                                }
                            }
                            None => {
                                self.throwSyntaxError()?;
                            }
                        }
                    }

                    // 后边应该到下边的 case "VALUES" 那边 因为rust的match默认有break效果不会到下边的case 需要使用猥琐的套路 把它们都包裹到loop
                }
                "VALUES" => { // values
                    let currentElement = self.getCurrentElementAdvance()?;
                    if currentElement.expectTextLiteralContentIgnoreCase("(") == false {
                        self.throwSyntaxError()?;
                    }

                    loop {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // columnValue 不能是TextLiteral
                        match currentElement {
                            Element::StringContent(stringContent) => {
                                let a = *&1;
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
                                    global::逗号_STR => {
                                        continue;
                                    }
                                    ")" => {
                                        break;
                                    }
                                    _ => {
                                        self.throwSyntaxErrorDetail("column value should not be text literal")?;
                                    }
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

        let currentElement = self.getCurrentElementAdvance()?;
        // link 后边应该是src的table name
        if let Some(srcTableName) = currentElement.expectTextLiteral() {
            link.srcTableName = srcTableName;
        } else {
            self.throwSyntaxErrorDetail("link should followed by table name")?;
        }

        let currentElement = self.getCurrentElementAdvance()?;
        match currentElement.expectTextLiteral() {
            Some(text) => {
                match text.to_uppercase().as_str() {
                    // 说明后边是表的筛选条件的
                    global::括号_STR => {
                        // 返回1个
                        suffix_minus_minus!(self.currentElementIndex);
                        let condition = self.parseCondition(true)?;
                        println!("aaaaaa");
                    }
                    "TO" => {
                        let currentElement = self.getCurrentElementAdvance()?;

                        // 后边应该是dest的table name
                        if let Some(destTableName) = currentElement.expectTextLiteral() {
                            link.destTableName = destTableName;
                        } else {
                            self.throwSyntaxErrorDetail("to should followed by dest table name when use link sql")?;
                        }
                    }
                    _ => {}
                }
            }
            None => {
                self.throwSyntaxError()?;
            }
        }

        Ok(Command::Link(link))
    }

    // ((id > 1 and level=6 and code in ('a')) and true and (name in ('a') or code = null))
    /// 当link sql解析到表名后边的"("时候 调用该函数 不过调用的时候elementIndex还是"("的前边1个
    fn parseCondition(&mut self, needDrain: bool) -> Result<Condition> {
        let mut condition = Condition::default();

        // 把该condition打头的"("消耗掉
        // assert!(self.getCurrentElementAdvance()?.expectTextLiteralContent(global::括号_STR));

        if self.getCurrentElement()?.expectTextLiteralContent(global::括号_STR) {
            if self.skipElement(1) {
                self.throwSyntaxErrorDetail("unexpected end of sql")?;
            }
        }

        let mut pendingElementVec: Vec<&Element> = Vec::new();

        enum ParseCondState {
            ParsingLeft,
            ParsingNoLogicalOp,
            ParsingRight,
            ParsingLogicalOp,
            ParsingSibling,
            ParseComplete,
        }

        let mut parseCondState = ParseCondState::ParsingLeft;

        // milestone 有 括号 Op
        // 当读取到")"时候 要是后边
        loop {
            let currentElement = match self.getCurrentElementOptionAdvance() {
                None => { break; }
                Some(currentElement) => { currentElement }
            };

            match parseCondState {
                ParseCondState::ParsingLeft => {
                    // 1上来便是 false true,本身便是个condition了
                    match currentElement {
                        Element::Boolean(bool) => {
                            // 替换
                            condition = Condition::falseTrueCondition(bool.clone());
                        }
                        _ => {
                            condition.left = currentElement.clone();
                        }
                    }

                    parseCondState = ParseCondState::ParsingNoLogicalOp;
                }
                ParseCondState::ParsingNoLogicalOp => {
                    if let Element::Op(op) = currentElement {
                        match op {
                            Op::MathCmpOp(mathCmpOp) => {
                                condition.op = Some(Op::MathCmpOp(mathCmpOp.clone()));
                            }
                            Op::SqlOp(sqlOp) => {
                                condition.op = Some(Op::SqlOp(sqlOp.clone()))
                            }
                            _ => {
                                self.throwSyntaxError()?;
                            }
                        }

                        parseCondState = ParseCondState::ParsingRight;
                    } else {
                        self.throwSyntaxError()?;
                    }
                }
                ParseCondState::ParsingRight => {
                    condition.right.push(currentElement.clone());

                    if self.peekNextElementOpt().is_some() {
                        if needDrain == false {
                            parseCondState = ParseCondState::ParseComplete;
                            break;
                        }

                        parseCondState = ParseCondState::ParsingLogicalOp;
                        // 其实不写break也是相同的 因为有上边的getCurrentElementOptionAdvance()保护
                        // break;
                    }
                }
                //  ParseCondState::AfterParsingRight => {}
                ParseCondState::ParsingLogicalOp => { // 说明1个非复合(例如简单的a=0)的condition 已然解析了
                    if let Element::Op(Op::LogicalOp(logicalOp)) = currentElement {
                        parseCondState = ParseCondState::ParsingSibling;
                    } else {
                        self.throwSyntaxErrorDetail("expect a logical op")?;
                    }
                }
                ParseCondState::ParsingSibling => {
                    // 是不是用()包裹的也是需要区分的
                    // 如果是的话那么是要嵌套的,如果不是的话是平铺的
                    let needDrain = currentElement.expectTextLiteralContent(global::括号_STR);

                    // 不管如何 返回前1个的index保持接下来的要parse的condition完全
                    self.currentElementIndex = self.currentElementIndex - 1;

                    let siblingCondition = self.parseCondition(needDrain)?;

                    condition.siblings.push((LogicalOp::And, Box::new(siblingCondition)));

                    parseCondState = ParseCondState::ParseComplete;
                    break;
                }
                _ => {}
            }
        }

        Ok(condition)
    }

    fn parseCondition0(&mut self) -> Result<ConditionEnum> {
        // 把该condition打头的"("消耗掉
        // assert!(self.getCurrentElementAdvance()?.expectTextLiteralContent(global::括号_STR));

        if self.getCurrentElement()?.expectTextLiteralContent(global::括号_STR) {
            if self.skipElement(1) {
                self.throwSyntaxErrorDetail("unexpected end of sql")?;
            }
        }

        enum ParseCondState {
            ParsingLeft,
            ParsingNoLogicalOp,
            ParsingLogicalOp,
            ParsingRight,
            ParseRightComplete,
        }

        let mut topLevelConditionEnum = ConditionEnum::default();

        let mut parseCondState = ParseCondState::ParsingLeft;

        loop {
            let currentElement = match self.getCurrentElementOptionAdvance() {
                None => {
                    break;
                }
                Some(currentElement) => {
                    currentElement
                }
            };

            match parseCondState {
                ParseCondState::ParsingLeft => {
                    match currentElement {
                        Element::TextLiteral(text) => {
                            if text == global::括号_STR {
                                suffix_minus_minus!(self.currentElementIndex);

                                // 能够得到个全的condition,然后落地到上级的left
                                topLevelConditionEnum = ConditionEnum::Composite {
                                    left: Box::new(self.parseCondition0()?),
                                    logicalOp: None,
                                    right: None,
                                };
                                parseCondState = ParseCondState::ParsingLogicalOp;

                                continue;
                            }
                        }
                        // true and a=1
                        // true = a
                        Element::Boolean(bool) => {
                            // 因为上边get但前的同时也会advance,故而getCurrent已然是next了
                            let nextElement = self.getCurrentElement()?;

                            // 可能是1个condition,也可能是base的1边
                            // 需要看看后边跟的是什么
                            match nextElement {
                                // true and a=1
                                Element::Op(Op::LogicalOp(_)) => {
                                    topLevelConditionEnum = ConditionEnum::Composite {
                                        left: Box::new(ConditionEnum::Bool(bool.clone())),
                                        logicalOp: None,
                                        right: None,
                                    };
                                    parseCondState = ParseCondState::ParsingLogicalOp;
                                }
                                // true = a
                                Element::Op(Op::MathCmpOp(_)) | Element::Op(Op::SqlOp(_)) => {
                                    topLevelConditionEnum = ConditionEnum::Base {
                                        left: Element::Boolean(bool.clone()),
                                        noLogicalOp: Op::Unknown,
                                        right: Vec::default(),
                                    };
                                    parseCondState = ParseCondState::ParsingNoLogicalOp;
                                }
                                _ => {
                                    self.throwSyntaxError()?;
                                }
                            }

                            continue;
                        }
                        _ => {}
                    }

                    topLevelConditionEnum = ConditionEnum::Base {
                        left: currentElement.clone(),
                        noLogicalOp: Op::Unknown,
                        right: Vec::default(),
                    };
                    parseCondState = ParseCondState::ParsingNoLogicalOp;
                }
                ParseCondState::ParsingNoLogicalOp => {
                    let mut ok = false;

                    match currentElement {
                        Element::Op(Op::MathCmpOp(mathCmpOp)) => {
                            if let ConditionEnum::Base { left, right, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Base {
                                    left,
                                    noLogicalOp: Op::MathCmpOp(mathCmpOp.clone()),
                                    right,
                                };
                                ok = true;
                            }
                        }
                        Element::Op(Op::SqlOp(sqlOp)) => {
                            if let ConditionEnum::Base { left, right, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Base {
                                    left,
                                    noLogicalOp: Op::SqlOp(sqlOp.clone()),
                                    right,
                                };
                                ok = true;
                            }
                        }
                        _ => {}
                    }

                    if ok == false {
                        self.throwSyntaxError()?;
                    }

                    parseCondState = ParseCondState::ParsingRight;
                }
                ParseCondState::ParsingLogicalOp => {
                    match currentElement {
                        Element::Op(Op::LogicalOp(logicalOp)) => {
                            if let ConditionEnum::Composite { left, right, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Composite {
                                    left,
                                    logicalOp: Some(logicalOp.clone()),
                                    right,
                                };
                                parseCondState = ParseCondState::ParsingRight;
                            }
                        }
                        _ => {
                            self.throwSyntaxError()?;
                        }
                    }
                }
                ParseCondState::ParsingRight => {
                    match currentElement {
                        Element::TextLiteral(text) => {
                            // 后续要支持 a in ('a') 和 a = (0+1)
                            if text == global::括号_STR {
                                suffix_minus_minus!(self.currentElementIndex);
                                let subCondition = self.parseCondition0()?;

                                // 要知道当前的topLevelCondition是哪类的
                                if let ConditionEnum::Composite { left, logicalOp, .. } = topLevelConditionEnum {
                                    topLevelConditionEnum = ConditionEnum::Composite {
                                        left,
                                        logicalOp,
                                        right: Some(Box::new(subCondition)),
                                    };
                                } else {
                                    self.throwSyntaxError()?;
                                }
                            }
                        }
                        Element::Boolean(bool) => {
                            if let ConditionEnum::Base { left, noLogicalOp, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Base {
                                    left,
                                    noLogicalOp,
                                    right: vec![Element::Boolean(bool.clone())],
                                }
                            } else if let ConditionEnum::Composite { left, logicalOp, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Composite {
                                    left,
                                    logicalOp,
                                    right: Some(Box::new(ConditionEnum::Bool(bool.clone()))),
                                }
                            }
                        }
                        _ => {
                            if let ConditionEnum::Base { left, noLogicalOp, .. } = topLevelConditionEnum {
                                topLevelConditionEnum = ConditionEnum::Base {
                                    left,
                                    noLogicalOp,
                                    right: vec![currentElement.clone()],
                                }
                            } else {
                                self.throwSyntaxError()?;
                            }
                        }
                    }

                    parseCondState = ParseCondState::ParseRightComplete;
                }
                ParseCondState::ParseRightComplete => {
                    match currentElement {
                        Element::TextLiteral(text) => {

                        }
                        Element::Op(Op::LogicalOp(logicalOp)) => {

                        }
                        _ => {

                        }
                    }
                }
            }
        }

        self.throwSyntaxError()?
    }

    /// 返回None的话说明当前已经是overflow了 和之前遍历char时候不同的是 当不能advance时候index是在最后的index还要向后1个的
    fn getCurrentElementOptionAdvance(&mut self) -> Option<&Element> {
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex);
        if option.is_some() {
            suffix_plus_plus!(self.currentElementIndex);
        }
        option
    }

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

    fn peekNextElement(&self) -> Result<&Element> {
        let option = self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex + 1);
        if option.is_some() {
            Ok(option.unwrap())
        } else {
            self.throwSyntaxErrorDetail("unexpected end of sql")?
        }
    }

    fn peekNextElementOpt(&self) -> Option<&Element> {
        self.elementVecVec.get(self.currentElementVecIndex).unwrap().get(self.currentElementIndex + 1)
    }

    /// 和parse toke 遍历char不同的是 要是越界了 index会是边界的后边1个 以符合当前的体系
    fn skipElement(&mut self, step: usize) -> bool {
        let currentElementVecLen = self.elementVecVec.get(self.currentElementVecIndex).unwrap().len();

        if self.currentElementIndex + step >= self.elementVecVec.get(self.currentElementVecIndex).unwrap().len() {
            self.currentElementIndex = currentElementVecLen;
            true
        } else {
            false
        }
    }

    /// 字母数字 且 数字不能打头
    fn checkDbObjectName(&self, name: &str) -> Result<()> {
        let chars: Vec<char> = name.chars().collect();

        // 打头得要字母
        let firstCharCorrect = match chars[0] {
            'a'..='z' => {}
            'A'..='Z' => {}
            _ => {
                self.throwSyntaxErrorDetail("table,column name should start with letter")?;
            }
        };

        if name.len() == 1 {
            return Ok(());
        }

        for char in chars[1..].iter() {
            match char {
                'a'..='z' => {}
                'A'..='Z' => {}
                '0'..='9' => {}
                _ => {
                    self.throwSyntaxErrorDetail("table,column name should only contain letter , number")?;
                }
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
    fn expectTextLiteral(&self) -> Option<String> {
        if let Element::TextLiteral(text) = self {
            Some(text.to_string())
        } else {
            None
        }
    }

    fn expectTextLiteralContent(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            content == expectContent
        } else {
            false
        }
    }

    fn expectTextLiteralContentIgnoreCase(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            let expectContent = expectContent.to_uppercase();
            let content = content.to_uppercase();

            expectContent == content
        } else {
            false
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
            Element::TextLiteral(s) => {
                write!(f, "{}({})", "TextLiteral", s)
            }
            Element::StringContent(s) => {
                write!(f, "{}({})", "StringContent", s)
            }
            Element::IntegerLiteral(s) => {
                write!(f, "{}({})", "IntegerLiteral", s)
            }
            Element::DecimalLiteral(s) => {
                write!(f, "{}({})", "DecimalLiteral", s)
            }
            Element::Boolean(bool) => {
                write!(f, "{}({})", "Boolean", bool)
            }
            Element::Op(op) => {
                write!(f, "{}({})", "Op", op)
            }
            _ => {
                write!(f, "{}", "Unknown")
            }
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

#[derive(Clone)]
pub enum Op {
    MathCmpOp(MathCmpOp),
    SqlOp(SqlOp),
    LogicalOp(LogicalOp),
    Unknown,
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::MathCmpOp(s) => {
                write!(f, "MathCmpOp({})", s)
            }
            Op::LogicalOp(s) => {
                write!(f, "LogicalOp({})", s)
            }
            Op::SqlOp(s) => {
                write!(f, "SqlOp({})", s)
            }
            _ => {
                write!(f, "Unknown")
            }
        }
    }
}

impl Default for Op {
    fn default() -> Self {
        Op::Unknown
    }
}

// https://note.qidong.name/2023/03/rust-enum-str/
#[derive(DisplayStrum, Clone)]
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
            global::等号_str => { MathCmpOp::Equal }
            global::小于_str => { MathCmpOp::LessThan }
            global::大于_str => { MathCmpOp::GreaterThan }
            global::小于等于 => { MathCmpOp::LessEqual }
            global::大于等于 => { MathCmpOp::GreaterEqual }
            global::不等_str => { MathCmpOp::NotEqual }
            _ => { MathCmpOp::Unknown }
        }
    }
}

#[derive(DisplayStrum, Clone)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(DisplayStrum, Clone)]
pub enum SqlOp {
    In,
    Is,
}

/// link user(id = 1) to car(color = 'red') by usage(number = 2)
#[derive(Default)]
pub struct Link {
    pub srcTableName: String,
    pub srcTableCondition: Option<Condition>,

    pub destTableName: String,
    pub destTableCondition: Option<Condition>,

    pub relationName: String,
    pub columnNames: Vec<String>,
    pub columnValues: Vec<ColumnValue>,
}

#[derive(Default)]
#[deprecated]
// ((a=1 and b=0)or(c=3 and d=6) and m ='0')
pub struct Condition {
    // Element 虽然能clone 不过是不是用 arc
    pub left: Element,
    pub op: Option<Op>,
    /// 为什么会是vec 因为需要应对 name in ('a','r')
    pub right: Vec<Element>,
    pub siblings: Vec<(LogicalOp, Box<Condition>)>,
}

impl Condition {
    pub fn falseTrueCondition(bool: bool) -> Condition {
        Condition {
            left: Element::Boolean(bool),
            op: None,
            right: Vec::new(),
            siblings: Vec::new(),
        }
    }
}

// ((a=1 and b=0)or(c=3 and d=6) and m ='0')
// 碰到"(" 下钻递归 和 碰到and,or 返回得到ConditionEnum 把它落地到上级的ConditionEnum的left
pub enum ConditionEnum {
    /// 如果是单单的 false true
    Bool(bool),
    /// 普通的 a=1
    Base {
        left: Element,
        noLogicalOp: Op,
        right: Vec<Element>,
    },
    Composite {
        left: Box<ConditionEnum>,
        // ((a=1 and b=0)) 那么顶级的condition的op和right都是None
        logicalOp: Option<LogicalOp>,
        right: Option<Box<ConditionEnum>>,
    },
}

impl Default for ConditionEnum {
    fn default() -> Self {
        ConditionEnum::Bool(true)
    }
}

pub struct ConditionBase {}

pub struct ConditionComposite {}

// ------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use crate::parser;

    #[test]
    pub fn testParseCreateTable() {
        let command = parser::parse("CREATE    TABLE    TEST   ( COLUMN1 string   ,  COLUMN2 DECIMAL)").unwrap();
    }

    #[test]
    pub fn testParseInsert() {
        // println!("{}", "".parse::<f64>().unwrap());
        parser::parse("insert   INTO TEST VALUES ( 0  , ')')").unwrap();
    }

    #[test]
    pub fn testLink() {
        "false".parse::<bool>().unwrap();
        parser::parse("link user(id > 1 and ( name = 'a' or code = 6)) to car(color='red') by usage(number = 13)").unwrap();
    }
}



