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
}

#[macro_export]
macro_rules! cols {
    ( $( $x:expr ),* )  => {{
        let mut v = Vec::new();
        $(
            v.push(Into::<$crate::column::Column>::into($x));
        )*
        v
    }};
}
