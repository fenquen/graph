use crate::{global, prefix_plus_plus, throw, throwFormat};
use crate::parser::command::Command;
use crate::parser::element::Element;
use anyhow::Result;

pub mod element;
pub mod command;
pub mod op;
mod expr;

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

pub fn parse(sql: &str) -> Result<Vec<Command>> {
    if sql.is_empty() {
        return Ok(vec![]);
    }

    let mut parser = Parser::new(sql);

    parser.parseElement()?;

    /*for elementVec in &parser.elementVecVec {
        for element in elementVec {
            println!("{element}");
        }

        println!();
    }*/

    parser.parse()
}

impl Parser {
    pub fn new(sql: &str) -> Self {
        let mut parser = Parser::default();
        parser.sql = sql.trim().to_string();
        parser.chars = parser.sql.chars().collect::<Vec<char>>();

        parser
    }

    fn parse(&mut self) -> Result<Vec<Command>> {
        let mut commandVec = Vec::new();

        loop {
            let command =
                match self.getCurrentElementAdvance()?.expectTextLiteral(global::EMPTY_STR)?.to_lowercase().as_str() {
                    "create" => self.parseCreate()?,
                    "drop" => self.parseDrop()?,
                    "insert" => self.parseInsert()?,
                    "link" => self.parseLink(false)?,
                    "delete" => self.parseDelete()?,
                    "update" => self.parseUpdate()?,
                    "select" => self.parseSelect()?,
                    "unlink" => self.parseUnlink()?,
                    "commit" => self.parseCommit()?,
                    "rollback" => self.parseRollback()?,
                    "set" => self.parseSet()?,
                    "show" => self.parseShow()?,
                    _ => self.throwSyntaxError()?,
                };

            log::debug!("{:?}\n", command);

            commandVec.push(command);

            // 到下个的elementVec
            if prefix_plus_plus!(self.currentElementVecIndex) >= self.elementVecVec.len() {
                break;
            }

            self.currentElementIndex = 0;
        }

        Ok(commandVec)
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

    fn throwSyntaxError<T>(&self) -> Result<T> {
        throwFormat!("syntax error, sql:{}", self.sql)
    }

    fn throwSyntaxErrorDetail<T>(&self, message: &str) -> Result<T> {
        throwFormat!("syntax error, sql:{}, {}", self.sql, message)
    }
}

#[cfg(test)]
mod test {
    use crate::parser;

    #[test]
    pub fn testParseCreateTable() {
        parser::parse("create table if not exist user (id integer not null default 0,name string)").unwrap();
    }

    #[test]
    pub fn testParseCreateIndex() {
        parser::parse("create index aaa on user[name,id]").unwrap();
    }

    #[test]
    pub fn testParseInsert() {
        parser::parse("insert into user values (1,null),(1,null)").unwrap();
    }

    #[test]
    pub fn testParseSelect() {
        // parser::parse("select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> wheel").unwrap();
        // parser::parse("select user(id >1 ) as user0 ,in usage (number = 7) ,as end in own(number =7)").unwrap();
        // parser::parse("select user(id = 1) -likes recursive(3..]-> user(age > 2)").unwrap();
        // parser::parse("select user as user0 limit 1 offset 0").unwrap();
        parser::parse("select user[id,name](id=1 and 0=6 and name like '%a')").unwrap();
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

    #[test]
    pub fn testAutocommit() {
        parser::parse("set autocommit true").unwrap();
    }

    #[test]
    pub fn testDrop() {
        parser::parse("drop relation a").unwrap();
    }
}
