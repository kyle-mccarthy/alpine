use crate::column::Column;
use crate::{as_array, ops::math, utils, ArrayRef, DataType, Field, View};
use arrow::array::PrimitiveArray;
use arrow::datatypes as dt;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The column does not exist {}", column))]
    InvalidColumn { column: Select },

    #[snafu(display(
        "The arithmetic operation {:?} cannot be done on the data type {:?}",
        op,
        data_type
    ))]
    InvalidArithmeticDataType { op: Arithmetic, data_type: DataType },

    #[snafu(display("The data types must be the same. {:?} != {:?}", lhs, rhs))]
    UnequalDataTypes { lhs: DataType, rhs: DataType },

    ArithmeticError {
        source: crate::error::arrow_error::ArrowError,
    },
}

pub enum Aggregate {
    Min,
    Max,
    Sum,
    Avg,
    Count,
}

#[derive(Debug, Clone)]
pub enum Arithmetic {
    Add,
    Sub,
    Div,
    Mul,
    // Mod,
}

impl Arithmetic {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Div => "/",
            Self::Mul => "*",
            // Self::Mod => "%",
        }
    }
}

pub enum Func {
    Arithmetic(Arithmetic, Column, Column),
    Aggregate(Aggregate, Column),
}

#[derive(Debug, Clone)]
pub enum Select {
    Column(Column),
    Alias(Box<Select>, String),
    Arithmetic(Arithmetic, Column, Column),
    //Aggregate(Column)
}

impl std::fmt::Display for Select {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Select::Column(column) => write!(fmt, "{}", column),
            Select::Alias(sel, name) => write!(fmt, "Alias({} as {})", sel, name),
            Select::Arithmetic(op, lhs, rhs) => write!(fmt, "{} {} {}", lhs, op.as_str(), rhs),
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

impl From<(Arithmetic, &str, &str)> for Select {
    fn from(s: (Arithmetic, &str, &str)) -> Select {
        Select::Arithmetic(s.0, s.1.into(), s.2.into())
    }
}

pub fn select(view: View, columns: Vec<Select>) -> Result<View, crate::Error> {
    // check for existance of the columns on the df
    if let Some(column) = columns.iter().find(|s| !column_exists(&view, s)) {
        return Err(Error::InvalidColumn {
            column: column.clone(),
        }
        .into());
    }

    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut data: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for sel in columns.iter() {
        let (field, array_ref) = select_index(&view, sel)?;
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
        Select::Arithmetic(_, lhs, rhs) => view.index_exists(&lhs) && view.index_exists(&rhs),
    }
}

fn select_index(view: &View, s: &Select) -> Result<(Field, ArrayRef), crate::Error> {
    match s {
        Select::Column(column) => view
            .subview(&column)
            .ok_or_else(|| Error::InvalidColumn { column: s.clone() }.into()),
        Select::Alias(sel, alias) => {
            if let Ok((field, array_ref)) = select_index(&view, sel) {
                // create the field with the new name -- arrow doesn't expose a set_name method and
                // the name prop is private
                let field = Field::new(
                    alias.as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                );
                return Ok((field, array_ref));
            }
            Err(Error::InvalidColumn { column: s.clone() }.into())
        }
        Select::Arithmetic(op, lhs, rhs) => apply_arithmetic(view, op, lhs, rhs),
    }
}

macro_rules! apply_arithmetic {
    ($lhs:ident, $rhs:ident, $op:ident, $([$dt:path, $ty:ty]),*) => {

        match $lhs.data_type() {
            $(
                $dt => {
                    let lhs_arr = as_array!($lhs, PrimitiveArray<$ty>).unwrap();
                    let rhs_arr = as_array!($rhs, PrimitiveArray<$ty>).unwrap();

                    (match $op {
                        Arithmetic::Add => math::add(lhs_arr, rhs_arr),
                        Arithmetic::Sub => math::subtract(lhs_arr, rhs_arr),
                        Arithmetic::Mul => math::multiply(lhs_arr, rhs_arr),
                        Arithmetic::Div => math::divide(lhs_arr, rhs_arr)
                    })
                },
            )+
            _ => unreachable!("should not reach this arithmetic op case - is_numeric_type should have denied already")
        }
    }
}

fn apply_arithmetic(
    view: &View,
    op: &Arithmetic,
    lhs: &Column,
    rhs: &Column,
) -> Result<(Field, ArrayRef), crate::Error> {
    let (lhs_field, lhs_arr) = view.subview(&lhs).ok_or(Error::InvalidColumn {
        column: Select::Column(lhs.clone()),
    })?;
    let (rhs_field, rhs_arr) = view.subview(&rhs).ok_or(Error::InvalidColumn {
        column: Select::Column(rhs.clone()),
    })?;

    if lhs_arr.data_type() != rhs_arr.data_type() {
        return Err(Error::UnequalDataTypes {
            lhs: lhs_arr.data_type().clone(),
            rhs: rhs_arr.data_type().clone(),
        }
        .into());
    }

    if !utils::is_numeric_type(lhs_arr.data_type()) {
        return Err(Error::InvalidArithmeticDataType {
            data_type: lhs_arr.data_type().clone(),
            op: op.clone(),
        }
        .into());
    }

    apply_arithmetic!(
        lhs_arr,
        rhs_arr,
        op,
        [DataType::UInt8, dt::UInt8Type],
        [DataType::UInt16, dt::UInt16Type],
        [DataType::UInt32, dt::UInt32Type],
        [DataType::UInt64, dt::UInt64Type],
        [DataType::Int8, dt::Int8Type],
        [DataType::Int16, dt::Int16Type],
        [DataType::Int32, dt::Int32Type],
        [DataType::Int64, dt::Int64Type],
        [DataType::Float32, dt::Float32Type],
        [DataType::Float64, dt::Float64Type]
    )
    .map(|array_ref| {
        let field = Field::new(
            &format!("{}_{}_{}", rhs_field.name(), op.as_str(), lhs_field.name()),
            array_ref.data_type().clone(),
            lhs_field.is_nullable() && rhs_field.is_nullable(),
        );
        (field, array_ref)
    })
}

#[cfg(test)]
mod test_select {
    use super::*;
    use crate::{array, col, sel, select, view};
    use arrow::array::{Array, PrimitiveArray};
    use arrow::datatypes as dt;

    #[test]
    fn it_selects_add() {
        let view = view!(
            ["a", dt::UInt8Type, [1, 2, 3, 4, 5]],
            ["b", dt::UInt8Type, [1, 2, 3, 4, 5]]
        );

        let res_view =
            select!(view, ["a", "b", sel!(sel!(Arithmetic::Add, "a", "b"), "c")]).unwrap();

        let c_col = res_view.column(&col!("c")).unwrap();

        assert_eq!(c_col.data(), array!(dt::UInt8Type, [2, 4, 6, 8, 10]).data());
    }
}
