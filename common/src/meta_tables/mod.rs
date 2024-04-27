use crate::Table;

pub mod scanned_ranges;

pub fn tables() -> Vec<Table> {
    vec![scanned_ranges::table()]
}
