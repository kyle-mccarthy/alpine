use crate::{DataFrame, Error, View};
use arrow::array::ArrayRef;

pub struct CsvReader {}

impl CsvReader {
    pub fn from_path(path: &str) -> Result<DataFrame, Error> {
        let file = std::fs::File::open(path)?;

        let mut reader = arrow::csv::ReaderBuilder::new()
            .infer_schema(Some(20))
            .has_headers(true)
            .build(file)?;

        let schema = reader.schema();
        let mut columns: Vec<ArrayRef> = vec![];

        while let Some(batch) = reader.next()? {
            for idx in 0..batch.num_columns() {
                let column: ArrayRef = batch.column(idx).to_owned();
                columns.push(column);
            }
        }

        Ok(DataFrame::new(View::new(
            schema.fields().to_owned(),
            columns,
        )))
    }
}

#[cfg(test)]
mod csv_test {
    use super::*;

    #[test]
    fn it_reads_from_path() {
        let path = "/Volumes/CODE/retl/examples/csv-example/sample-csv.csv";
        let df = CsvReader::from_path(path).unwrap();

        // dbg!(data.schema());
        // dbg!(data.row(1));

        // let col = data.column::<Int64Array, &str>("beds").unwrap();

        // dbg!(col.value(1));

        //let df = crate::dataframe::DataFrame::new(data);
        //df.select(crate::cols!("beds", 0));

        // let df = df.select(vec![sel!("sq__ft", "sq_ft")]).unwrap();
    }
}
