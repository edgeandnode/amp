use std::sync::Arc;

use crate::{
    arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, StructBuilder},
    catalog::physical::PhysicalTable,
    timestamp_type, Timestamp, TimestampArrayBuilder,
};
use datafusion::arrow::{
    array::{ArrayBuilder, BinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    error::ArrowError,
};

use crate::Table;

pub const TABLE_NAME: &'static str = "__datasets";

lazy_static::lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(schema());
}

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
    }
}

fn schema() -> Schema {
    let name = Field::new("name", DataType::Utf8, false);
    let network = Field::new("network", DataType::Utf8, false);
    let url = Field::new("url", DataType::Utf8, false);
    let sql_query = Field::new("sql_query", DataType::Utf8, false);
    let query_plan = Field::new("query_plan", DataType::Binary, false);
    let created_at = Field::new("created_at", timestamp_type(), false);
    let dependencies = Field::new(
        "dependencies",
        DataType::List(Arc::new(Field::new("dependencies", DataType::Utf8, false))),
        false,
    );
    let tables = Field::new(
        "tables",
        DataType::List(Arc::new(table_struct_field())),
        false,
    );
    let fields = vec![
        name,
        network,
        url,
        sql_query,
        query_plan,
        created_at,
        dependencies,
        tables,
    ];
    Schema::new(fields)
}

fn column_struct_fields() -> Vec<Field> {
    vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
    ]
}

fn table_struct_field() -> Field {
    Field::new(
        "tables",
        DataType::Struct(Fields::from(table_struct_inner_fields())),
        false,
    )
}

fn table_struct_inner_fields() -> Vec<Field> {
    vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new(
            "sorted_by",
            DataType::List(Arc::new(Field::new("sorted_by", DataType::Utf8, false))),
            false,
        ),
        Field::new(
            "schema",
            DataType::List(Arc::new(Field::new(
                "schema",
                DataType::Struct(Fields::from(column_struct_fields())),
                false,
            ))),
            false,
        ),
    ]
}

pub struct DatasetRow {
    pub name: String,
    pub network: String,
    pub url: String,
    pub sql_query: String,
    pub query_plan: Vec<u8>,
    pub created_at: Timestamp,
    pub dependencies: Vec<String>,
    pub tables: Vec<TableStruct>,
}

impl DatasetRow {
    pub fn to_record_batch(&self) -> RecordBatch {
        let mut builder = DatasetRowsBuilder::new();
        builder.append(self);
        builder.flush().unwrap()
    }
}

pub struct TableStruct {
    pub name: String,
    pub url: String,
    pub sorted_by: Vec<String>,
    pub schema: Vec<ColumnStruct>,
}

impl From<PhysicalTable> for TableStruct {
    fn from(table: PhysicalTable) -> Self {
        let sorted_by = table.table.sorted_by();
        let PhysicalTable {
            url,
            table_ref,
            table: Table { name: _, schema },
        } = table;
        let fields = schema.fields().iter();
        let schema: Vec<ColumnStruct> = fields
            .map(|field| ColumnStruct {
                column_name: field.name().clone(),
                data_type: serde_json::to_string(field.data_type()).unwrap(),
                is_nullable: field.is_nullable(),
            })
            .collect();

        TableStruct {
            name: table_ref.to_string(),
            url: url.to_string(),
            sorted_by,
            schema,
        }
    }
}

pub struct ColumnStruct {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[derive(Debug)]
pub struct DatasetRowsBuilder {
    name: StringBuilder,
    network: StringBuilder,
    url: StringBuilder,
    sql_query: StringBuilder,
    query_plan: BinaryBuilder,
    dependencies: ListBuilder<StringBuilder>,
    created_at: TimestampArrayBuilder,
    tables: ListBuilder<StructBuilder>,
}

impl DatasetRowsBuilder {
    pub fn new() -> Self {
        let table_fields = table_struct_inner_fields();

        Self {
            name: StringBuilder::new(),
            network: StringBuilder::new(),
            url: StringBuilder::new(),
            sql_query: StringBuilder::new(),
            query_plan: BinaryBuilder::new(),
            dependencies: ListBuilder::new(StringBuilder::new()).with_field(Field::new(
                "dependencies",
                DataType::Utf8,
                false,
            )),
            created_at: TimestampArrayBuilder::with_capacity(0),
            tables: ListBuilder::new(StructBuilder::from_fields(table_fields, 0))
                .with_field(table_struct_field()),
        }
    }

    pub fn append(&mut self, dataset: &DatasetRow) {
        let DatasetRow {
            name,
            network,
            url,
            sql_query,
            query_plan,
            created_at,
            dependencies,
            tables,
        } = dataset;

        self.name.append_value(name);
        self.network.append_value(network);
        self.url.append_value(url);
        self.sql_query.append_value(sql_query);
        self.query_plan.append_value(query_plan);
        self.created_at.append_value(*created_at);

        for dependency in dependencies {
            let values = self.dependencies.values().as_any_mut();
            let string_values = values.downcast_mut::<StringBuilder>().unwrap();
            string_values.append_value(dependency);
        }
        self.dependencies.append(true);

        let table_builder = self.tables.values();
        for table in tables {
            let TableStruct {
                name,
                url,
                sorted_by,
                schema,
            } = table;

            table_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(name);
            table_builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(url);
            let sorted_by_builder = table_builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                .unwrap();
            for sorted_by in sorted_by {
                let values = sorted_by_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                values.append_value(&sorted_by);
            }
            sorted_by_builder.append(true);

            let schemas = table_builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                .unwrap();
            for column in schema {
                let ColumnStruct {
                    column_name,
                    data_type,
                    is_nullable,
                } = column;

                let struct_builder = schemas
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();

                struct_builder
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(column_name);
                struct_builder
                    .field_builder::<StringBuilder>(1)
                    .unwrap()
                    .append_value(data_type);
                struct_builder
                    .field_builder::<BooleanBuilder>(2)
                    .unwrap()
                    .append_value(*is_nullable);
                struct_builder.append(true);
            }
            schemas.append(true);
            table_builder.append(true);
        }
        self.tables.append(true);
    }

    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let DatasetRowsBuilder {
            name,
            network,
            url,
            sql_query,
            query_plan,
            dependencies,
            created_at,
            tables,
        } = self;

        let columns = vec![
            Arc::new(name.finish()) as ArrayRef,
            Arc::new(network.finish()),
            Arc::new(url.finish()),
            Arc::new(sql_query.finish()),
            Arc::new(query_plan.finish()),
            Arc::new(created_at.finish()),
            Arc::new(dependencies.finish()),
            Arc::new(tables.finish()),
        ];

        RecordBatch::try_new(SCHEMA.clone(), columns)
    }

    pub fn len(&self) -> usize {
        self.name.len()
    }
}

#[test]
fn dataset_rows_builder() {
    use datafusion::arrow::util::pretty::pretty_format_batches;

    // Define the test dataset
    let dataset = DatasetRow {
        name: "TestDataset".to_string(),
        network: "mainnet".to_string(),
        url: "http://example.com".to_string(),
        sql_query: "SELECT * FROM BaseDataset".to_string(),
        query_plan: vec![],
        dependencies: vec!["BaseDataset".to_string()],
        created_at: Timestamp::default(),
        tables: vec![
            TableStruct {
                name: "Table1".to_string(),
                url: "http://example.com/table1".to_string(),
                sorted_by: vec!["column1".to_string(), "column2".to_string()],
                schema: vec![
                    ColumnStruct {
                        column_name: "column1".to_string(),
                        data_type: "Int64".to_string(),
                        is_nullable: false,
                    },
                    ColumnStruct {
                        column_name: "column2".to_string(),
                        data_type: "String".to_string(),
                        is_nullable: true,
                    },
                ],
            },
            TableStruct {
                name: "Table2".to_string(),
                url: "http://example.com/table2".to_string(),
                sorted_by: vec!["columnA".to_string()],
                schema: vec![ColumnStruct {
                    column_name: "columnA".to_string(),
                    data_type: "Boolean".to_string(),
                    is_nullable: false,
                }],
            },
        ],
    };

    let mut builder = DatasetRowsBuilder::new();
    builder.append(&dataset);
    let record_batch = builder.flush().unwrap();

    let formatted = pretty_format_batches(&[record_batch.clone()])
        .unwrap()
        .to_string();

    let expected_output = "
+-------------+---------+--------------------+---------------------------+------------+----------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name        | network | url                | sql_query                 | query_plan | created_at           | dependencies  | tables                                                                                                                                                                                                                                                                                                                                                                |
+-------------+---------+--------------------+---------------------------+------------+----------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TestDataset | mainnet | http://example.com | SELECT * FROM BaseDataset |            | 1970-01-01T00:00:00Z | [BaseDataset] | [{name: Table1, url: http://example.com/table1, sorted_by: [column1, column2], schema: [{column_name: column1, data_type: Int64, is_nullable: false}, {column_name: column2, data_type: String, is_nullable: true}]}, {name: Table2, url: http://example.com/table2, sorted_by: [columnA], schema: [{column_name: columnA, data_type: Boolean, is_nullable: false}]}] |
+-------------+---------+--------------------+---------------------------+------------+----------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
";

    assert_eq!(formatted.trim(), expected_output.trim());
}
