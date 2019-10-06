use arrow::error::ArrowError as ArrowErr;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum ArrowError {
    MemoryError { inner: String },
    ParseError { inner: String },
    ComputeError { inner: String },
    DivideByZero,
    CsvError { inner: String },
    JsonError { inner: String },
    IoError { inner: String },
    InvalidArgumentError { inner: String },
}

impl From<ArrowErr> for ArrowError {
    fn from(error: ArrowErr) -> ArrowError {
        match error {
            ArrowErr::MemoryError(inner) => ArrowError::MemoryError { inner },
            ArrowErr::ParseError(inner) => ArrowError::ParseError { inner },
            ArrowErr::ComputeError(inner) => ArrowError::ComputeError { inner },
            ArrowErr::DivideByZero => ArrowError::DivideByZero,
            ArrowErr::CsvError(inner) => ArrowError::CsvError { inner },
            ArrowErr::JsonError(inner) => ArrowError::JsonError { inner },
            ArrowErr::IoError(inner) => ArrowError::IoError { inner },
            ArrowErr::InvalidArgumentError(inner) => ArrowError::InvalidArgumentError { inner },
        }
    }
}

impl From<ArrowErr> for super::Error {
    fn from(error: ArrowErr) -> super::Error {
        let source = Into::<ArrowError>::into(error);
        super::Error::Arrow { source }
    }
}
