use crate::query::select::Select;
use crate::query::Query;
use crate::view::View;
use crate::Error;

#[derive(Clone, Debug)]
pub struct DataFrame {
    view: View,
}

impl DataFrame {
    pub fn new(view: View) -> DataFrame {
        DataFrame { view }
    }

    pub fn query(self) -> Query<'static> {
        Query::new(self.view)
    }

    pub fn select(self, columns: Vec<Select>) -> Result<DataFrame, Error> {
        Ok(Query::new(self.view).select(columns).exec()?.to_df())
    }

    pub fn view(&self) -> &View {
        &self.view
    }
}
