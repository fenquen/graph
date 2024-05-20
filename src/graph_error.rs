use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub struct GraphError {
    message: String,
}

impl Debug for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "message:{}", self.message)
    }
}


impl Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "message:{}", self.message)
    }
}

impl GraphError {
    pub fn new(message: &str) -> GraphError {
        GraphError {
            message: message.to_string()
        }
    }
}


impl Error for GraphError {}