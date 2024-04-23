use std::cmp::PartialEq;
use std::fmt::{Debug, Display, Formatter, Pointer, write};
use std::ptr::read;
use std::sync::Arc;
use std::vec;
use crate::{Column, ColumnType, global, parser, prefix_plus_plus, suffix_plus_plus, Table, TableType, throw};
use anyhow::{Result};
use serde_json::to_string;

pub fn parse(sql: &str) -> Result<Vec<Command>> {
    let mut parser = Parser::new(sql);

    parser.parseElement()?;

    for elementVec in &parser.elementVecVec {
        for element in elementVec {
            println!("{}", element);
        }
    }

    parser.parse()
}

pub enum Command {
    CreateTable(Table),
    INSERT,
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

    elementVecVec: Vec<Vec<Element>>,
    currentElementVecIndex: usize,
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
            let mut advanceCount: usize = 0;

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

                    advanceCount = 1;
                }
                global::单引号_CHAR => {
                    if self.whetherIn单引号() {
                        match self.nextChar() {
                            // 说明是末尾了,下边的文本结束是相同的 select a where name = 'a'
                            None => {
                                self.collectPendingChars(&mut currentElementVec);

                                self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                                advanceCount = 1;
                            }
                            Some(nextChar) => {
                                // 连续的2个 单引号 对应1个
                                if nextChar == global::单引号_CHAR {
                                    self.pendingChars.push(currentChar);
                                    advanceCount = 2;
                                } else { // 说明文本结束的
                                    self.collectPendingChars(&mut currentElementVec);

                                    self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                                    advanceCount = 1;
                                }
                            }
                        }
                    } else {
                        // 开启了1个文本读取 需要把老的了结掉
                        self.collectPendingChars(&mut currentElementVec);

                        self.单引号as文本边界的数量 = self.单引号as文本边界的数量 + 1;
                        advanceCount = 1;
                    }
                }
                global::括号_CHAR | global::括号1_CHAR | global::逗号_CHAR => {
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else {
                        self.collectPendingChars(&mut currentElementVec);
                        currentElementVec.push(Element::TextLiteral(currentChar.to_string()));
                    }

                    advanceCount = 1;

                    if currentChar == global::括号_CHAR {
                        self.括号数量 = self.括号数量 + 1;
                    } else if currentChar == global::括号1_CHAR {
                        self.括号1数量 = self.括号1数量 + 1;
                    }
                }
                global::分号_char => { // 应对同时写了多个的sql
                    // 单纯的是文本内容
                    if self.whetherIn单引号() {
                        self.pendingChars.push(currentChar);
                    } else { // 说明是多个的sql的分隔的
                        self.collectPendingChars(&mut currentElementVec);

                        if currentElementVec.len() > 0 {
                            self.elementVecVec.push(currentElementVec);
                            currentElementVec = Vec::new();
                        }
                    }

                    advanceCount = 1;
                }
                _ => {
                    self.pendingChars.push(currentChar);
                    advanceCount = 1;
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

        // text是纯数字
        let element = if isPureNumberText {
            // 当前是不是在单引号的包围 是文本
            if self.whetherIn单引号() {
                Element::TextLiteral(text)
            } else {
                if isDecimal {
                    Element::DecimalLiteral(text.parse::<f64>().unwrap())
                } else {
                    Element::IntegerLiteral(text.parse::<i64>().unwrap())
                }
            }
        } else {
            Element::TextLiteral(text)
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
                            self.parseCreateTable(TableType::from(text))
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
                Element::IntegerLiteral(_) | Element::DecimalLiteral(_) => {
                    self.throwSyntaxErrorDetail("column name , column type can not be pure number")?;
                }
            }
        }

        Ok(Command::CreateTable(table))
    }

    fn parseInsert(&mut self) -> Result<Command> {
        Ok(Command::UNKNOWN)
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
            self.throwSyntaxError()?
        }
    }
}

pub enum Element {
    TextLiteral(String),
    IntegerLiteral(i64),
    DecimalLiteral(f64),
}

impl Element {
    fn expectTextLiteral(&self) -> bool {
        if let Element::TextLiteral(_) = self {
            true
        } else {
            false
        }
    }

    fn expectTextLiteralContent(&self, expectContent: &str) -> bool {
        if let Element::TextLiteral(content) = self {
            content == expectContent
        } else {
            false
        }
    }
}

impl Display for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Element::TextLiteral(s) => {
                write!(f, "{}({})", "TextLiteral", s)
            }
            Element::IntegerLiteral(s) => {
                write!(f, "{}({})", "IntegerLiteral", s)
            }
            Element::DecimalLiteral(s) => {
                write!(f, "{}({})", "DecimalLiteral", s)
            }
            _ => {
                write!(f, "{}", "UNKNOWN")
            }
        }
    }
}

impl Debug for Element {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

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
        let a: usize = 0;
        // println!("{:?}", vec![1].get(a - 1));
        // println!("{}", "".parse::<f64>().unwrap());
        // parser::parse("insert   INTO TEST VALUES ( '0'  , 0'7").unwrap();
    }
}


