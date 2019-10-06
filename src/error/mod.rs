use snafu::Snafu;

pub mod arrow_error;
use arrow::datatypes::DataType;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    Arrow {
        source: arrow_error::ArrowError,
    },
    Io {
        source: std::io::Error,
    },
    FromIntError {
        source: std::num::TryFromIntError,
    },
    IndexOutOfBounds {
        index: usize,
        len: usize,
    },
    WrongType,
    UnknownDataType {
        data_type: arrow::datatypes::DataType,
    },
    InvalidDowncast,
    InvalidOperation {
        op: String,
        lhs: DataType,
        rhs: DataType,
    },
    FromStrError {
        value: String,
        description: Option<&'static str>,
    },
    InvalidByteLength {
        len: usize,
        expected: usize,
    },
    InvalidByteAlignment,
    InvalidByteSliceLength {
        len: usize,
    },
    SelectError {
        source: crate::query::select::Error,
    },
    FilterError {
        source: crate::query::filter::Error,
    },
    ComparisonError {
        source: crate::ops::cmp::Error,
    },
}

macro_rules! impl_from_source {
    ($from:ty, $to:path) => {
        impl From<$from> for crate::Error {
            fn from(source: $from) -> crate::Error {
                $to { source }
            }
        }
    };
}

impl_from_source!(arrow_error::ArrowError, Error::Arrow);
impl_from_source!(std::io::Error, Error::Io);
impl_from_source!(std::num::TryFromIntError, Error::FromIntError);
impl_from_source!(crate::query::select::Error, Error::SelectError);
impl_from_source!(crate::query::filter::Error, Error::FilterError);
impl_from_source!(crate::ops::cmp::Error, Error::ComparisonError);
