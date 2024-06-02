use crate::{global, prefix_plus_plus, throw};
use crate::parser::command::Command;
use crate::parser::element::Element;

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

pub fn parse(sql: &str) -> anyhow::Result<Vec<Command>> {
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

impl Parser {
    pub fn new(sql: &str) -> Self {
        let mut parser = Parser::default();
        parser.sql = sql.to_string();
        parser.chars = parser.sql.chars().collect::<Vec<char>>();

        parser
    }

    fn parse(&mut self) -> anyhow::Result<Vec<Command>> {
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

    fn throwSyntaxError<T>(&self) -> anyhow::Result<T> {
        throw!(&format!("syntax error, sql:{}", self.sql))
    }

    fn throwSyntaxErrorDetail<T>(&self, message: &str) -> anyhow::Result<T> {
        throw!(&format!("syntax error, sql:{}, {}", self.sql, message))
    }
}

#[cfg(test)]
mod test {
    use crate::parser0;
    use crate::parser0::Parser;

    #[test]
    pub fn testParseCreateTable() {
        parser0::parse("CREATE    TABLE    TEST   ( COLUMN1 string null  ,  COLUMN2 DECIMAL null)").unwrap();
    }

    #[test]
    pub fn testParseInsert() {
        // println!("{}", "".parse::<f64>().unwrap());
        parser0::parse("insert into user values (1,null)").unwrap();
    }

    #[test]
    pub fn testParseSelect() {
        // parser::parse("select user[id,name](id=1 and 0=6) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> wheel").unwrap();
        parser0::parse("select user(id >1 ) as user0 ,in usage (number = 7) ,as end in own(number =7)").unwrap();
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
        parser0::parse("link user ( a in ((a = 1) = true)) to company (id > 1 and ( name = 'a' or code = 1 + 0 and true)) by usage(a=0,a=1212+0,d=1)").unwrap();
    }

    #[test]
    pub fn testUpdate() {
        parser0::parse("update user[name='a',order=7]").unwrap();
    }

    #[test]
    pub fn testParseDelete() {
        parser0::parse("delete from user(a=0)").unwrap();
    }

    #[test]
    pub fn testUnlink() {
        //parser::parse("unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13)").unwrap();
        parser0::parse("unlink user(id >1 ) as start by usage (number = 7) ,as end by own(number =7)").unwrap();
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

        parser0::parse(sql).unwrap();
    }
}
