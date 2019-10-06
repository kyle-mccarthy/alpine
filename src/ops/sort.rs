use crate::error::Error;
use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowNumericType;
use std::str::FromStr;

pub enum Sort {
    ASC,
    DESC,
}

impl FromStr for Sort {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if unicase::eq(s, "asc") {
            return Ok(Sort::ASC);
        }

        if unicase::eq(s, "desc") {
            return Ok(Sort::DESC);
        }

        Err(Error::FromStrError {
            value: s.to_string(),
            description: Some("Allowed options are asc or desc"),
        })
    }
}

impl Sort {
    pub fn sort<T: ArrowNumericType>(arr: PrimitiveArray<T>) {}
}
