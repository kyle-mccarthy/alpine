use arrow::array::{Array, ArrayRef, BinaryArray, PrimitiveArray};

use arrow::datatypes::{DataType, Field, Schema};
use snafu::ensure;
use std::collections::HashMap;
use std::sync::Arc;

use crate::datatype::ScalarValue;
use crate::error::{self as error, Error};
use crate::{as_array, Idx};

#[derive(Clone, Debug)]
pub struct Data {
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
    index: HashMap<String, usize>,
}

impl Data {
    pub fn new(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Data {
        let mut index = HashMap::new();

        schema.fields().iter().enumerate().for_each(|(i, f)| {
            index.insert(f.name().clone(), i);
        });

        Data {
            schema,
            columns,
            index,
        }
    }

    pub fn column<I: Into<Idx>>(&self, index: I) -> Result<ArrayRef, Error> {
        let index = self.field_index(index)?;
        Ok(self.columns[index].clone())
    }

    pub fn typed_column<T: Array + 'static, I: Into<Idx>>(&self, index: I) -> Result<&T, Error> {
        let index = self.field_index(index)?;

        self.columns[index]
            .as_any()
            .downcast_ref::<T>()
            .ok_or(Error::WrongType)
    }

    pub fn column_type(&self, index: usize) -> Result<&DataType, Error> {
        ensure!(
            self.columns.len() > index,
            error::IndexOutOfBounds {
                index,
                len: self.columns.len()
            }
        );

        Ok(self.columns[0].data_type())
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn field_index<I: Into<Idx>>(&self, index: I) -> Result<usize, Error> {
        let index = index.into();

        match &index {
            Idx::Pos(i) => {
                if *i < self.columns.len() {
                    return Ok(*i);
                }
                Err(Error::IndexOutOfBounds {
                    index: *i,
                    len: self.columns.len(),
                })
            }
            Idx::Name(name) => self
                .index
                .get(name)
                .copied()
                .ok_or(Error::IndexDoesNotExist { index }),
        }
    }

    pub fn field<I: Into<Idx>>(&self, index: I) -> Option<&Field> {
        let index = self.field_index(index).ok()?;
        Some(self.schema.field(index))
    }

    pub fn len(&self) -> usize {
        self.columns[0].len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn row<'a>(&'a self, index: usize) -> Result<Vec<ScalarValue<'a>>, Error> {
        ensure!(
            index < self.len(),
            error::IndexOutOfBounds {
                index,
                len: self.len()
            }
        );

        let mut row: Vec<ScalarValue<'a>> = Vec::with_capacity(self.columns.len());

        for i in 0..self.columns.len() {
            row.push(self.value_scalar(index, i)?);
        }

        Ok(row)
    }

    /// perform a cast on the specified index, does not update the data, produces a new array
    pub fn cast<I: Into<Idx> + Clone>(
        &self,
        index: I,
        to_type: &DataType,
    ) -> Result<ArrayRef, Error> {
        let index = self.field_index(index.clone())?;

        let casted_data = arrow::compute::cast(&self.columns[index], to_type)?;

        Ok(casted_data)
    }

    pub fn value_scalar(&self, row: usize, col: usize) -> Result<ScalarValue<'_>, Error> {
        // do a bounds check
        ensure!(
            row < self.len(),
            error::IndexOutOfBounds {
                index: row,
                len: self.len()
            }
        );
        ensure!(
            col < self.columns.len(),
            error::IndexOutOfBounds {
                index: col,
                len: self.columns.len()
            }
        );

        let column = &self.columns[col];

        if column.is_null(row) {
            return Ok(ScalarValue::Null);
        }

        let data_type = column.data_type();

        match data_type {
            DataType::Utf8 => {
                // TODO are utf8s always in a BinaryArray or can they be in a ListArray too?
                let array = column.as_any().downcast_ref::<BinaryArray>().ok_or(
                    Error::UnknownDataType {
                        data_type: data_type.clone(),
                    },
                )?;

                ScalarValue::from_bytes(data_type, array.value(row))
            }
            DataType::Boolean => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::BooleanType>)?;
                Ok(ScalarValue::Boolean(array.value(row)))
            }
            DataType::Int8 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Int8Type>)?;
                Ok(ScalarValue::Int8(array.value(row)))
            }
            DataType::Int16 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Int16Type>)?;
                Ok(ScalarValue::Int16(array.value(row)))
            }
            DataType::Int32 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Int32Type>)?;
                Ok(ScalarValue::Int32(array.value(row)))
            }
            DataType::Int64 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Int64Type>)?;
                Ok(ScalarValue::Int64(array.value(row)))
            }
            DataType::UInt8 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::UInt8Type>)?;
                Ok(ScalarValue::UInt8(array.value(row)))
            }
            DataType::UInt16 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::UInt16Type>)?;
                Ok(ScalarValue::UInt16(array.value(row)))
            }
            DataType::UInt32 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::UInt32Type>)?;
                Ok(ScalarValue::UInt32(array.value(row)))
            }
            DataType::UInt64 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::UInt64Type>)?;
                Ok(ScalarValue::UInt64(array.value(row)))
            }
            DataType::Float32 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Float32Type>)?;
                Ok(ScalarValue::Float32(array.value(row)))
            }
            DataType::Float64 => {
                let array = as_array!(column, PrimitiveArray<arrow::datatypes::Float64Type>)?;
                Ok(ScalarValue::Float64(array.value(row)))
            }
            _ => Err(Error::UnknownDataType {
                data_type: data_type.clone(),
            }),
        }
    }
}
