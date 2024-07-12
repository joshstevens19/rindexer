use std::fmt::Display;

#[derive(Debug)]
pub struct Code(String);

impl From<String> for Code {
    fn from(value: String) -> Self {
        Code(value)
    }
}

impl Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Code {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_string(&self) -> String {
        self.0.to_string()
    }

    pub fn push_str(&mut self, value: &Code) {
        self.0.push_str(&value.0);
    }

    pub fn new(value: String) -> Self {
        Code(value)
    }

    pub fn blank() -> Self {
        Code(String::new())
    }
}
