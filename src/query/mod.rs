pub mod filter;
pub mod select;

use crate::{Error, View};
use filter::Filter;
use select::Select;

pub struct Query<'a> {
    view: View,
    select: Vec<Select>,
    filter: Option<Filter<'a>>,
}

impl<'a> Query<'a> {
    pub fn new(view: View) -> Query<'a> {
        Query {
            view,
            select: vec![],
            filter: None,
        }
    }

    pub fn select(mut self, select: Vec<Select>) -> Query<'a> {
        self.select = select;
        self
    }

    pub fn filter(mut self, filter: Filter<'a>) -> Query<'a> {
        self.filter = Some(filter);
        self
    }

    pub fn exec(self) -> Result<View, Error> {
        let view = select::select(self.view, self.select)?;

        Ok(view)
    }
}
