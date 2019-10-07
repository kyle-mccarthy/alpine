use crate::column::Column;
use crate::DataFrame;
use arrow::{array::ArrayRef, datatypes::Field};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct View {
    columns: Vec<ArrayRef>,
    fields: Vec<Field>,
    indexes: HashMap<String, usize>,
}

impl View {
    pub fn new(fields: Vec<Field>, columns: Vec<ArrayRef>) -> View {
        let mut indexes = HashMap::with_capacity(fields.len());

        fields.iter().enumerate().for_each(|(i, f)| {
            indexes.insert(f.name().clone(), i);
        });

        assert_eq!(fields.len(), columns.len());

        View {
            indexes,
            fields,
            columns,
        }
    }

    pub fn get_index(&self, index: &Column) -> Option<usize> {
        let index: usize = (match index {
            Column::Name(name) => self.indexes.get(name).copied(),
            Column::Position(position) => Some(*position),
        })?;

        if self.columns.len() > index {
            Some(index)
        } else {
            None
        }
    }

    pub fn index_exists(&self, index: &Column) -> bool {
        self.get_index(index).is_some()
    }

    pub fn full_index(&self, column: &Column) -> Option<(usize, &Field)> {
        let index = self.get_index(column)?;
        let field = &self.fields[index];
        Some((index, field))
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn field(&self, index: &Column) -> Option<&Field> {
        let index = self.get_index(index)?;
        Some(&self.fields[index])
    }

    pub fn field_mut(&mut self, index: &Column) -> Option<&mut Field> {
        let index = self.get_index(index)?;
        Some(&mut self.fields[index])
    }

    pub fn column(&self, index: &Column) -> Option<ArrayRef> {
        let index = self.get_index(index)?;
        Some(self.columns[index].clone())
    }

    /// panics if index out of bounds
    pub fn column_unchecked(&self, index: usize) -> ArrayRef {
        self.columns[index].clone()
    }

    pub fn subview(&self, index: &Column) -> Option<(Field, ArrayRef)> {
        if let Some(position) = self.get_index(index) {
            return Some((
                self.fields[position].clone(),
                self.columns[position].clone(),
            ));
        }
        None
    }

    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            return 0;
        }
        self.columns[0].len()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn to_df(self) -> DataFrame {
        DataFrame::new(self)
    }

    pub fn columns(&self) -> &Vec<ArrayRef> {
        &self.columns
    }
}
