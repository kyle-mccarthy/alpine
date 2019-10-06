use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use std::any::Any;

#[derive(Clone, Debug)]
pub struct Array {
    data: ArrayRef,
}

impl Array {
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.data.is_null(index)
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.data.is_valid(index)
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn data_type(&self) -> &DataType {
        self.data.data_type()
    }

    pub fn is_numeric_type(&self) -> bool {
        match self.data.data_type() {
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

    pub fn is_temporal_type(&self) -> bool {
        match self.data.data_type() {
            DataType::Timestamp(_)
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Interval(_) => true,
            _ => false,
        }
    }

    pub fn is_arrow_numeric_type(&self) -> bool {
        self.is_numeric_type() || self.is_temporal_type()
    }
}

trait Abc: arrow::array::Array {}
