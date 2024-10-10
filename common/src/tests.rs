use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::arrow::{
    array::UInt64Array,
    datatypes::{Field, Schema},
};

use super::*;

#[test]
fn empty_dataset_rows() {
    // No tables.
    let dataset_rows = DatasetRows(Vec::new());
    assert!(dataset_rows.is_empty());
    assert!(dataset_rows.block_num().is_err());

    // Table has `block_num`, but no values.
    let field = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let schema = SchemaRef::new(Schema::new(vec![field]));
    let table = Table {
        name: "table".to_string(),
        schema: schema.clone(),
        network: None,
    };
    let column = Arc::new(UInt64Array::from(Vec::<u64>::new()));
    let table_rows = TableRows::new(table, vec![column]).unwrap();
    let dataset_rows = DatasetRows(vec![table_rows]);
    assert!(dataset_rows.is_empty());
    assert!(dataset_rows.block_num().is_err());

    // Table does not have `block_num` column.
    let field = Field::new("not_block_num", DataType::UInt64, false);
    let schema = SchemaRef::new(Schema::new(vec![field]));
    let table = Table {
        name: "table".to_string(),
        schema: schema.clone(),
        network: None,
    };
    let column = Arc::new(UInt64Array::from(vec![1, 2, 3]));
    let table_rows = TableRows::new(table, vec![column]).unwrap();
    let dataset_rows = DatasetRows(vec![table_rows]);
    assert!(!dataset_rows.is_empty());
    assert!(dataset_rows.block_num().is_err());
}

#[test]
fn dataset_rows_unify_block_num() {
    let field = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let schema = SchemaRef::new(Schema::new(vec![field]));
    let blocks_table = Table {
        name: "blocks".to_string(),
        schema: schema.clone(),
        network: None,
    };
    let logs_table = Table {
        name: "logs".to_string(),
        schema: schema.clone(),
        network: None,
    };

    let dataset_rows_from_nums = |block_nums: Vec<u64>, logs_nums: Vec<u64>| {
        let blocks_column = Arc::new(UInt64Array::from(block_nums));
        let logs_column = Arc::new(UInt64Array::from(logs_nums));
        let blocks_rows = TableRows::new(blocks_table.clone(), vec![blocks_column]).unwrap();
        let logs_rows = TableRows::new(logs_table.clone(), vec![logs_column]).unwrap();
        DatasetRows(vec![blocks_rows, logs_rows])
    };

    let dataset_rows = dataset_rows_from_nums(vec![1], vec![]);
    assert!(!dataset_rows.is_empty());
    assert_eq!(dataset_rows.block_num().unwrap(), 1);

    let dataset_rows = dataset_rows_from_nums(vec![1], vec![1]);
    assert_eq!(dataset_rows.block_num().unwrap(), 1);

    let dataset_rows = dataset_rows_from_nums(vec![5, 5], vec![]);
    assert_eq!(dataset_rows.block_num().unwrap(), 5);

    let dataset_rows = dataset_rows_from_nums(vec![1, 2], vec![]);
    assert!(dataset_rows.block_num().is_err());

    let dataset_rows = dataset_rows_from_nums(vec![1], vec![2]);
    assert!(dataset_rows.block_num().is_err());

    let dataset_rows = dataset_rows_from_nums(vec![1, 2, 3], vec![]);
    assert!(dataset_rows.block_num().is_err());

    let dataset_rows = dataset_rows_from_nums(vec![1, 2], vec![2]);
    assert!(dataset_rows.block_num().is_err());

    let dataset_rows = dataset_rows_from_nums(vec![54, 54], vec![54]);
    assert_eq!(dataset_rows.block_num().unwrap(), 54);

    let dataset_rows = dataset_rows_from_nums(vec![44, 44], vec![44, 44, 44]);
    assert_eq!(dataset_rows.block_num().unwrap(), 44);
}
