use crate::column::Column;
use crate::{ArrayRef, Field, View};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The column does not exist {}", column))]
    InvalidColumn { column: Select },
}

#[derive(Debug, Clone)]
pub enum Select {
    Column(Column),
    Alias(Box<Select>, String),
}

impl std::fmt::Display for Select {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Select::Column(column) => write!(fmt, "{}", column),
            Select::Alias(sel, name) => write!(fmt, "Alias({} as {})", sel, name),
        }
    }
}

impl From<usize> for Select {
    fn from(s: usize) -> Select {
        Select::Column(Column::Position(s))
    }
}

impl From<String> for Select {
    fn from(s: String) -> Select {
        Select::Column(Column::Name(s))
    }
}

impl From<&str> for Select {
    fn from(s: &str) -> Select {
        Select::Column(Column::Name(s.to_string()))
    }
}

pub fn select(view: View, columns: Vec<Select>) -> Result<View, Error> {
    // check for existance of the columns on the df
    if let Some(column) = columns.iter().find(|s| !column_exists(&view, s)) {
        return Err(Error::InvalidColumn {
            column: column.clone(),
        });
    }

    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut data: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for sel in columns.iter() {
        let (field, array_ref) = select_index(&view, sel).unwrap();
        // fields get a new numeric index after a select, the numeric index relates to the position
        // within the columns arg
        fields.push(field);
        data.push(array_ref);
    }

    Ok(View::new(fields, data))
}

fn column_exists(view: &View, s: &Select) -> bool {
    match s {
        Select::Column(column) => view.index_exists(&column),
        Select::Alias(sel, _) => column_exists(&view, sel),
    }
}

fn select_index(view: &View, s: &Select) -> Option<(Field, ArrayRef)> {
    match s {
        Select::Column(column) => view.subview(&column),
        Select::Alias(sel, alias) => {
            if let Some((field, array_ref)) = select_index(&view, sel) {
                // create the field with the new name -- arrow doesn't expose a set_name method and
                // the name prop is private
                let field = Field::new(
                    alias.as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                );
                return Some((field, array_ref));
            }
            None
        }
    }
}
