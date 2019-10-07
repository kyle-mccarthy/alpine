#[macro_export]
macro_rules! as_array {
    ($arr:ident, $arr_type:ty) => {{
        $arr.as_any()
            .downcast_ref::<$arr_type>()
            .ok_or(crate::Error::InvalidDowncast)
    }};
}

#[macro_export]
macro_rules! sel {
    ($s:expr) => {{
        Into::<$crate::query::select::Select>::into($s)
    }};
    ($s:expr, $alias:expr) => {{
        $crate::query::select::Select::Alias(Box::new(sel!($s)), Into::<String>::into($alias))
    }};
    ($arith:path, $lhs:expr, $rhs:expr) => {{
        $crate::query::select::Select::Arithmetic(
            $arith,
            Into::<$crate::column::Column>::into($lhs),
            Into::<$crate::column::Column>::into($rhs),
        )
    }};
}

#[macro_export]
macro_rules! select {
    ($view:ident, [ $( $y:expr ),* ]) => {{
        use $crate::query::select::{Select, select};

        let mut sel_columns: Vec<Select> = vec![];

        $( sel_columns.push($crate::sel!($y)); )*

        select($view, sel_columns)
    }}
}

#[macro_export]
macro_rules! columns {
    ( $( $x:expr ),* )  => {{
        let mut v = Vec::new();
        $(
            v.push($crate::col!($x));
        )*
        v
    }};
}

#[macro_export]
macro_rules! col {
    ($x:expr) => {{
        Into::<$crate::column::Column>::into($x)
    }};
}

#[macro_export]
macro_rules! field {
    ($name:expr, $data_type:expr) => {{
        $crate::Field::new($name, $data_type, false)
    }};
    ($name:expr, $data_type:path, $is_nullable:ident) => {{
        $crate::Field::new($name, $data_type, $is_nullable)
    }};
}

#[macro_export]
macro_rules! array {
    ($ty:ty, $values:expr) => {{
        Into::<PrimitiveArray<$ty>>::into($values.to_vec())
    }};
    ($values:expr) => {{
        use arrow::array::BinaryArray;

        Into::<BinaryArray>::into($values.to_vec())
    }};
}

#[macro_export]
macro_rules! subview {
    ($name:expr, $path:expr, $values:expr) => {{
        ($crate::field!($name, $path), $crate::array_ref!($values))
    }};
    ($name:expr, $ty:ty, $values:expr) => {{
        use arrow::datatypes::ArrowPrimitiveType;
        (
            $crate::field!($name, <$ty as ArrowPrimitiveType>::get_data_type()),
            $crate::array_ref!($ty, $values),
        )
    }};
}

#[macro_export]
macro_rules! view {
    ($([$name:expr, $ty:ty, $values:expr]),*) => {{
        use $crate::{Field, ArrayRef};

        let mut fields: Vec<Field> = vec![];
        let mut arrays: Vec<ArrayRef> = vec![];

        $(
            let (field, array) = $crate::subview!($name, $ty, $values);
            fields.push(field);
            arrays.push(array);
        )*

        $crate::View::new(fields, arrays)
    }}
}

#[macro_export]
macro_rules! array_ref {
    ($ty:ty, $v:expr) => {{
        std::sync::Arc::new($crate::array!($ty, $v)) as arrow::array::ArrayRef
    }};
}
