use crate::View;
use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum Column {
    Name(String),
    Position(usize),
}

impl From<usize> for Column {
    fn from(i: usize) -> Column {
        Column::Position(i)
    }
}

impl From<&str> for Column {
    fn from(i: &str) -> Column {
        Column::Name(i.to_owned())
    }
}

impl Column {
    pub fn validate(&self, view: &View) -> bool {
        view.index_exists(self)
    }

    pub fn compile(self, view: &View) -> Option<CompiledColumn> {
        if !view.index_exists(&self) {
            return None;
        }

        let index = view.get_index(&self).unwrap();
        let field = view.field(&self).unwrap();

        Some(CompiledColumn::new(field.name().clone(), index))
    }
}

impl Display for Column {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Column::Name(name) => write!(fmt, "Name({})", name),
            Column::Position(position) => write!(fmt, "Position({})", position),
        }
    }
}

pub struct CompiledColumn {
    name: String,
    index: usize,
}

impl CompiledColumn {
    pub fn new(name: String, index: usize) -> CompiledColumn {
        CompiledColumn { name, index }
    }
}
