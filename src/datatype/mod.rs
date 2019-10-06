use crate::error::{self as error, Error};
use arrow::datatypes::DataType;
use bstr::BStr;
use std::fmt::Debug;

use snafu::ensure;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ScalarValue<'a> {
    Boolean(bool),

    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    Float32(f32),
    Float64(f64),

    Binary(&'a [u8]),
    String(&'a BStr),

    Null,
}

impl<'a> ScalarValue<'a> {
    pub fn from_bytes(data_type: &DataType, bytes: &'a [u8]) -> Result<ScalarValue<'a>, Error> {
        match data_type {
            DataType::Utf8 => Ok(ScalarValue::String(bytes.into())),
            DataType::UInt8 => from_bytes::<u8>(bytes).map(|v| ScalarValue::UInt8(*v)),
            DataType::UInt16 => from_bytes::<u16>(bytes).map(|v| ScalarValue::UInt16(*v)),
            DataType::UInt32 => from_bytes::<u32>(bytes).map(|v| ScalarValue::UInt32(*v)),
            DataType::UInt64 => from_bytes::<u64>(bytes).map(|v| ScalarValue::UInt64(*v)),

            DataType::Int8 => from_bytes::<i8>(bytes).map(|v| ScalarValue::Int8(*v)),
            DataType::Int16 => from_bytes::<i16>(bytes).map(|v| ScalarValue::Int16(*v)),
            DataType::Int32 => from_bytes::<i32>(bytes).map(|v| ScalarValue::Int32(*v)),
            DataType::Int64 => from_bytes::<i64>(bytes).map(|v| ScalarValue::Int64(*v)),

            DataType::Float32 => from_bytes::<f32>(bytes).map(|v| ScalarValue::Float32(*v)),
            DataType::Float64 => from_bytes::<f64>(bytes).map(|v| ScalarValue::Float64(*v)),

            _ => Err(Error::UnknownDataType {
                data_type: data_type.clone(),
            }),
        }
    }

    pub fn data_type(&self) -> Option<&DataType> {
        match self {
            Self::Boolean(_) => Some(&DataType::Boolean),

            Self::UInt8(_) => Some(&DataType::UInt8),
            Self::UInt16(_) => Some(&DataType::UInt16),
            Self::UInt32(_) => Some(&DataType::UInt32),
            Self::UInt64(_) => Some(&DataType::UInt64),

            Self::Int8(_) => Some(&DataType::Int8),
            Self::Int16(_) => Some(&DataType::Int16),
            Self::Int32(_) => Some(&DataType::Int32),
            Self::Int64(_) => Some(&DataType::Int64),

            Self::Float32(_) => Some(&DataType::Float32),
            Self::Float64(_) => Some(&DataType::Float64),

            Self::String(_) => Some(&DataType::Utf8),

            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        self == &ScalarValue::Null
    }

    pub fn as_uint8(&self) -> Option<u8> {
        match self {
            Self::UInt8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_uint8(&self) -> bool {
        match self {
            Self::UInt8(_) => true,
            _ => false,
        }
    }

    pub fn as_uint16(&self) -> Option<u16> {
        match self {
            Self::UInt16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_uint16(&self) -> bool {
        match self {
            Self::UInt16(_) => true,
            _ => false,
        }
    }

    pub fn as_uint32(&self) -> Option<u32> {
        match self {
            Self::UInt32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_uint32(&self) -> bool {
        match self {
            Self::UInt32(_) => true,
            _ => false,
        }
    }

    pub fn as_uint64(&self) -> Option<u64> {
        match self {
            Self::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_uint64(&self) -> bool {
        match self {
            Self::UInt64(_) => true,
            _ => false,
        }
    }

    pub fn as_int8(&self) -> Option<i8> {
        match self {
            Self::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_int8(&self) -> bool {
        match self {
            Self::Int8(_) => true,
            _ => false,
        }
    }

    pub fn as_int16(&self) -> Option<i16> {
        match self {
            Self::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_int16(&self) -> bool {
        match self {
            Self::Int16(_) => true,
            _ => false,
        }
    }

    pub fn as_int32(&self) -> Option<i32> {
        match self {
            Self::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_int32(&self) -> bool {
        match self {
            Self::Int32(_) => true,
            _ => false,
        }
    }

    pub fn as_int64(&self) -> Option<i64> {
        match self {
            Self::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_int64(&self) -> bool {
        match self {
            Self::Int64(_) => true,
            _ => false,
        }
    }

    pub fn as_float32(&self) -> Option<f32> {
        match self {
            Self::Float32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_float32(&self) -> bool {
        match self {
            Self::Float32(_) => true,
            _ => false,
        }
    }

    pub fn as_float64(&self) -> Option<f64> {
        match self {
            Self::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_float64(&self) -> bool {
        match self {
            Self::Float64(_) => true,
            _ => false,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_boolean(&self) -> bool {
        match self {
            Self::Boolean(_) => true,
            _ => false,
        }
    }
}

pub fn from_bytes<T: Debug + Copy>(bytes: &[u8]) -> Result<&T, Error> {
    ensure!(
        std::mem::size_of::<T>() > 0 && bytes.len() == std::mem::size_of::<T>(),
        error::InvalidByteLength {
            len: bytes.len(),
            expected: std::mem::size_of::<T>()
        }
    );
    ensure!(
        (bytes.as_ptr() as usize) % std::mem::align_of::<T>() == 0,
        error::InvalidByteAlignment
    );
    Ok(unsafe { &*(bytes.as_ptr() as *const T) })
}

#[cfg(test)]
mod test_scalars {
    use super::*;

    #[test]
    fn test_from_bytes() {
        let mut builder = arrow::array::UInt64Array::builder(5);
        builder.append_value(1024).unwrap();
        builder.append_value(2).unwrap();
        builder.append_value(3).unwrap();
        builder.append_value(4).unwrap();
        builder.append_value(5).unwrap();

        let array = builder.finish();
        let values = array.values();
        let array_bytes = values.data();

        let out = from_bytes::<u64>(&array_bytes[0..8]).unwrap();
        assert_eq!(out, &1024);
    }
}
