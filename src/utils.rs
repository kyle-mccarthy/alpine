use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

pub fn is_numeric_type(arr: &ArrayRef) -> bool {
    match arr.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => true,
        _ => false,
    }
}

pub fn is_temporal_type(arr: &ArrayRef) -> bool {
    match arr.data_type() {
        DataType::Timestamp(_)
        | DataType::Date32(_)
        | DataType::Date64(_)
        | DataType::Interval(_) => true,
        _ => false,
    }
}

pub fn is_arrow_numeric_type(arr: &ArrayRef) -> bool {
    is_numeric_type(&arr) || is_temporal_type(&arr)
}
