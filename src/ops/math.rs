use crate::Error;
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    PrimitiveArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{ArrowNumericType, DataType};
use std::sync::Arc;

macro_rules! array_op {
    ($lhs:expr, $rhs:expr, $op:ident, $op_name:expr) => {{
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::UInt8, DataType::UInt8) => do_op!($lhs, $rhs, $op, UInt8Array),
            (DataType::UInt16, DataType::UInt16) => do_op!($lhs, $rhs, $op, UInt16Array),
            (DataType::UInt32, DataType::UInt32) => do_op!($lhs, $rhs, $op, UInt32Array),
            (DataType::UInt64, DataType::UInt64) => do_op!($lhs, $rhs, $op, UInt64Array),
            (DataType::Int8, DataType::Int8) => do_op!($lhs, $rhs, $op, Int8Array),
            (DataType::Int16, DataType::Int16) => do_op!($lhs, $rhs, $op, Int16Array),
            (DataType::Int32, DataType::Int32) => do_op!($lhs, $rhs, $op, Int32Array),
            (DataType::Int64, DataType::Int64) => do_op!($lhs, $rhs, $op, Int64Array),
            (DataType::Float32, DataType::Float32) => do_op!($lhs, $rhs, $op, Float32Array),
            (DataType::Float64, DataType::Float64) => do_op!($lhs, $rhs, $op, Float64Array),
            (_, _) => Err(Error::InvalidOperation {
                op: $op_name.to_string(),
                lhs: $lhs.data_type().clone(),
                rhs: $rhs.data_type().clone(),
            }),
        }
    }};
}

macro_rules! do_op {
    ($lhs:expr, $rhs:expr, $op:ident, $dt:ident) => {{
        let lhs = $lhs
            .as_any()
            .downcast_ref::<$dt>()
            .ok_or($crate::Error::InvalidDowncast)?;
        let rhs = $rhs
            .as_any()
            .downcast_ref::<$dt>()
            .ok_or($crate::Error::InvalidDowncast)?;
        Ok(Arc::new(compute::$op(lhs, rhs)?) as ArrayRef)
    }};
}

pub fn add<T: ArrowNumericType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<ArrayRef, Error> {
    array_op!(lhs, rhs, add, "add")
}

pub fn subtract<T: ArrowNumericType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<ArrayRef, Error> {
    array_op!(lhs, rhs, subtract, "subtract")
}

pub fn divide<T: ArrowNumericType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<ArrayRef, Error> {
    array_op!(lhs, rhs, divide, "divide")
}

pub fn multiply<T: ArrowNumericType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<ArrayRef, Error> {
    array_op!(lhs, rhs, multiply, "multiply")
}

// pub fn modulo<T: ArrowNumericType>(
//     lhs: PrimitiveArray<T>,
//     rhs: PrimitiveArray<T>,
// ) -> Result<ArrayRef, Error> {
//     compute::math_op(&lhs, &rhs, |l, r| Ok(l % r))
//         .map(|arr| Arc::new(arr) as ArrayRef)
//         .map_err(|err| err.into())
// }
