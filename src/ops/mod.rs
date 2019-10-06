pub mod cmp;
pub mod math;

use crate::datatype::ScalarValue;
use arrow::datatypes::DataType;
use std::sync::Arc;

pub enum Expr<'a> {
    Column(usize),
    Literal(ScalarValue<'a>),
    Comparison(Arc<Expr<'a>>, Op, Arc<Expr<'a>>),
    IsNull(Arc<Expr<'a>>),
    IsNotNull(Arc<Expr<'a>>),
    Function(&'a str, &'a [Expr<'a>], DataType),
    // OrderBy(usize, Sort),
}

pub enum Op {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}
