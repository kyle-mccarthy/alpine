use arrow::array::BooleanArray;
use arrow::array::PrimitiveArray;
use arrow::compute::kernels::comparison as cmp;
use arrow::datatypes::ArrowNumericType;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};

use crate::error;
use crate::{Array, DataType};

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    InvalidComparison {
        lhs: DataType,
        rhs: DataType,
        op: CmpOp,
    },
    UnequalLength {
        lhs: usize,
        rhs: usize,
    },
}

#[derive(Clone, PartialEq)]
pub enum CmpOp {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
    NotEq,
}

impl CmpOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Eq => "==",
            CmpOp::NotEq => "!=",
        }
    }

    pub fn eq_ord(&self, order: Ordering) -> bool {
        match (self, order) {
            (Self::Gt, Ordering::Greater) => true,
            (Self::GtEq, Ordering::Greater) => true,
            (Self::GtEq, Ordering::Equal) => true,
            (Self::Eq, Ordering::Equal) => true,
            (Self::LtEq, Ordering::Equal) => true,
            (Self::LtEq, Ordering::Less) => true,
            (Self::Lt, Ordering::Less) => true,
            (_, _) => false,
        }
    }
}

impl Display for CmpOp {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmt, "{}", self.as_str())
    }
}

impl Debug for CmpOp {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmt, "{}", self.as_str())
    }
}

pub fn cmp_arrays<T: ArrowNumericType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: CmpOp,
) -> Result<BooleanArray, error::Error> {
    (match op {
        CmpOp::NotEq => cmp::neq(lhs, rhs),
        CmpOp::Eq => cmp::eq(lhs, rhs),
        CmpOp::Lt => cmp::lt(lhs, rhs),
        CmpOp::LtEq => cmp::lt_eq(lhs, rhs),
        CmpOp::Gt => cmp::gt(lhs, rhs),
        CmpOp::GtEq => cmp::gt_eq(lhs, rhs),
    })
    .map_err(|source| error::Error::Arrow {
        source: source.into(),
    })
}

pub fn cmp_bool_arrays(
    lhs: &BooleanArray,
    rhs: &BooleanArray,
    op: CmpOp,
) -> Result<BooleanArray, error::Error> {
    if lhs.len() != rhs.len() {
        return Err(Error::UnequalLength {
            lhs: lhs.len(),
            rhs: rhs.len(),
        }
        .into());
    }

    match op {
        CmpOp::Eq => Ok(cmp_bool(lhs, rhs, |a, b| a == b)),
        CmpOp::NotEq => Ok(cmp_bool(lhs, rhs, |a, b| a != b)),
        _ => Err(Error::InvalidComparison {
            lhs: lhs.data_type().clone(),
            rhs: rhs.data_type().clone(),
            op,
        }
        .into()),
    }
}

fn cmp_bool<F>(lhs: &BooleanArray, rhs: &BooleanArray, f: F) -> BooleanArray
where
    F: Fn(bool, bool) -> bool,
{
    let mut out = BooleanArray::builder(lhs.len());

    for i in 0..lhs.len() {
        out.append_value(f(lhs.value(i), rhs.value(i)))
            .expect("should append value");
    }

    out.finish()
}
