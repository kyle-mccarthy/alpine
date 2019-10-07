use crate::column::Column;
use crate::datatype::ScalarValue;
use crate::ops::cmp::{cmp_arrays, cmp_bool_arrays, CmpOp};
use crate::{as_array, DataType, View};

use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use arrow::compute::kernels::boolean;
use arrow::datatypes as dt;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    InvalidColumn {
        column: Column,
    },
    InvalidComparison {
        lhs: DataType,
        rhs: DataType,
        op: CmpOp,
    },
    #[snafu(display("ScalarValue does not have"))]
    InvalidScalarType,
    #[snafu(display(
        "Null values can only be compared with the Eq or NotEq operator, operator used {:?}",
        op
    ))]
    InvalidNullComparison {
        op: CmpOp,
    },
}

#[derive(Debug)]
pub enum Filter<'a> {
    Columns(Column, CmpOp, Column),
    Scalar(Column, CmpOp, ScalarValue<'a>),
    And(Box<Filter<'a>>, Box<Filter<'a>>),
    Or(Box<Filter<'a>>, Box<Filter<'a>>),
}

pub fn filter<'a>(view: &View, f: Filter<'a>) -> Result<View, crate::Error> {
    let bool_array = filter_view(view, f)?;

    let res: Result<Vec<ArrayRef>, _> = view
        .columns()
        .iter()
        .map(|arr| apply_filter(&arr, &bool_array))
        .collect();

    Ok(View::new(view.fields().clone(), res?))
}

fn filter_view<'a>(view: &View, f: Filter<'a>) -> Result<BooleanArray, crate::Error> {
    match f {
        Filter::And(lhs, rhs) => {
            let lhs_res = filter_view(&view, *lhs)?;
            let rhs_res = filter_view(&view, *rhs)?;
            boolean::and(&lhs_res, &rhs_res).map_err(|e| e.into())
        }
        Filter::Or(lhs, rhs) => {
            let lhs_res = filter_view(&view, *lhs)?;
            let rhs_res = filter_view(&view, *rhs)?;
            boolean::or(&lhs_res, &rhs_res).map_err(|e| e.into())
        }
        Filter::Columns(lhs, op, rhs) => filter_cols(&view, lhs, rhs, op),
        Filter::Scalar(arr, op, value) => filter_scalar(&view, arr, value, op),
    }
}

macro_rules! filter_cols {
    ($lhs:ident, $rhs:ident, $op:ident, $( [$dt:path, $ty:ty] ),*) => {
        match ($lhs.data_type(), $rhs.data_type(), $op) {
            $( ($dt, $dt, op) => {
                cmp_arrays(
                    as_array!($lhs, PrimitiveArray<$ty>).unwrap(),
                    as_array!($rhs, PrimitiveArray<$ty>).unwrap(),
                    op
                )
            } ,)+
        (DataType::Boolean, DataType::Boolean, CmpOp::Eq) => cmp_bool_arrays(
            as_array!($lhs, PrimitiveArray<dt::BooleanType>).unwrap(),
            as_array!($rhs, PrimitiveArray<dt::BooleanType>).unwrap(),
            CmpOp::Eq
        ),
        (DataType::Boolean, DataType::Boolean, CmpOp::NotEq) => cmp_bool_arrays(
            as_array!($lhs, PrimitiveArray<dt::BooleanType>).unwrap(),
            as_array!($rhs, PrimitiveArray<dt::BooleanType>).unwrap(),
            CmpOp::NotEq
        ),
        (lhs_dt, rhs_dt, op) => Err(Error::InvalidComparison {
                lhs: lhs_dt.clone(),
                rhs: rhs_dt.clone(),
                op,
            }
            .into()),
        }
    }
}

fn filter_cols(
    view: &View,
    lhs: Column,
    rhs: Column,
    op: CmpOp,
) -> Result<BooleanArray, crate::Error> {
    let lhs_idx = view
        .get_index(&lhs)
        .ok_or(Error::InvalidColumn { column: lhs })?;
    let rhs_idx = view
        .get_index(&rhs)
        .ok_or(Error::InvalidColumn { column: rhs })?;

    let lhs_arr = view.column_unchecked(lhs_idx);
    let rhs_arr = view.column_unchecked(rhs_idx);

    filter_cols!(
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
}

macro_rules! apply_filter {
    ($arr:ident, $bool_arr:ident, $( [$dt:path, $ty:ty] ),*) => {
        match $arr.data_type() {
            $($dt => arrow_filter(as_array!($arr, PrimitiveArray<$ty>).unwrap(), $bool_arr),)+
             missing_impl => unimplemented!(
                "filter has not yet been implemented for arrays of type {:?}",
                missing_impl),
        }
    };
}

fn apply_filter(arr: &ArrayRef, bool_arr: &BooleanArray) -> Result<ArrayRef, crate::Error> {
    use arrow::compute::array_ops::filter as arrow_filter;
    apply_filter!(
        arr,
        bool_arr,
        [DataType::Boolean, dt::BooleanType],
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
    .map_err(|e| e.into())
}

macro_rules! cmp_scalar {
    ($arr:ident, $scalar:ident, $bool_arr:ident, $op:ident, $( [$dt:path,  $ty:ty, $fn:ident] ),*) => {
        match($arr.data_type(), $scalar.data_type().unwrap())  {
            $(($dt, $dt) => {
                let arr = as_array!($arr, PrimitiveArray<$ty>).unwrap();
                let value = $scalar.$fn().unwrap();

                for i in 0..arr.len() {
                    $bool_arr.append_value($op.eq_ord(
                        arr.value(i).partial_cmp(&value).unwrap()
                    ))?;
                }

                Ok($bool_arr.finish())
            },)+
        (lhs, rhs) => Err(Error::InvalidComparison {
                lhs: lhs.clone(),
                rhs: rhs.clone(),
                op: $op,
            }.into()),
        }
    };
}

fn filter_scalar<'a>(
    view: &View,
    column: Column,
    value: ScalarValue<'a>,
    op: CmpOp,
) -> Result<BooleanArray, crate::error::Error> {
    if value.data_type().is_none() {
        return Err(Error::InvalidScalarType.into());
    }

    let arr_idx = view
        .get_index(&column)
        .ok_or(Error::InvalidColumn { column })?;
    let arr = view.column_unchecked(arr_idx);

    // check for IS/IS NOT NULL
    if value.is_null() {
        return match op {
            CmpOp::Eq | CmpOp::NotEq => {
                // array of arr[index] IS NULL
                let mut bool_arr = BooleanArray::builder(arr.len());

                for i in 0..arr.len() {
                    bool_arr.append_value(arr.is_null(i))?;
                }

                let bool_arr = bool_arr.finish();

                if op == CmpOp::Eq {
                    return Ok(bool_arr);
                }

                Ok(boolean::not(&bool_arr)?)
            }
            _ => Err(Error::InvalidNullComparison { op }.into()),
        };
    }

    let mut bool_arr = BooleanArray::builder(arr.len());

    cmp_scalar!(
        arr,
        value,
        bool_arr,
        op,
        [DataType::Boolean, dt::BooleanType, as_boolean],
        [DataType::UInt8, dt::UInt8Type, as_uint8],
        [DataType::UInt16, dt::UInt16Type, as_uint16],
        [DataType::UInt32, dt::UInt32Type, as_uint32],
        [DataType::UInt64, dt::UInt64Type, as_uint64],
        [DataType::Int8, dt::Int8Type, as_int8],
        [DataType::Int16, dt::Int16Type, as_int16],
        [DataType::Int32, dt::Int32Type, as_int32],
        [DataType::Int64, dt::Int64Type, as_int64],
        [DataType::Float32, dt::Float32Type, as_float32],
        [DataType::Float64, dt::Float64Type, as_float64]
    )
}

#[cfg(test)]
mod test_filter {
    use super::*;
    use crate::{array, view, DataType, Field, View};
    use arrow::array::UInt8Array;
    use arrow::datatypes as dt;
    use std::sync::Arc;

    #[test]
    fn it_filters() {
        let view = view!(
            ["a", dt::UInt8Type, [2, 4, 5, 8, 10]],
            ["b", dt::UInt8Type, [2, 4, 6, 8, 10]]
        );

        // let view = View::new(vec![a_col, b_col], vec![a_data, b_data]);

        {
            let filtered_view =
                filter(&view, Filter::Columns("a".into(), CmpOp::Eq, "b".into())).unwrap();

            assert_eq!(
                filtered_view.column(&"a".into()).unwrap().data(),
                filtered_view.column(&"b".into()).unwrap().data()
            );
        }

        {
            let filtered_view = filter(
                &view,
                Filter::Scalar("a".into(), CmpOp::GtEq, ScalarValue::UInt8(4u8)),
            )
            .unwrap();

            assert_eq!(filtered_view.column(&"a".into()).unwrap().len(), 4);

            let a = filtered_view.column(&"a".into()).unwrap();
            let expected_a = array!(dt::UInt8Type, [4, 5, 8, 10]);
            assert_eq!(as_array!(a, UInt8Array).unwrap(), &expected_a);

            assert_eq!(filtered_view.column(&"b".into()).unwrap().len(), 4);
            let b = filtered_view.column(&"b".into()).unwrap();
            let expected_b = array!(dt::UInt8Type, [4, 6, 8, 10]);
            assert_eq!(as_array!(b, UInt8Array).unwrap(), &expected_b);
        }
    }
}
