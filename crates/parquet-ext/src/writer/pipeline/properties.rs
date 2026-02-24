use std::{env::var_os, num::NonZeroUsize, sync::Arc, thread::available_parallelism};

use arrow_schema::SchemaRef;
use parquet::{
    arrow::{ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata},
    errors::{ParquetError, Result},
    file::properties::{WriterProperties, WriterPropertiesPtr},
    schema::types::SchemaDescPtr,
};

use crate::writer::pipeline::job::{EncoderInbox, EncoderOutbox, WriterInbox, WriterOutbox};

pub struct PipelineProperties {
    pub(in crate::writer) num_workers: usize,
    pub(in crate::writer) arrow_schema: SchemaRef,
    pub(in crate::writer) parquet_schema: SchemaDescPtr,
    pub(in crate::writer) channel_capacity: usize,
    pub(in crate::writer) writer_properties: WriterPropertiesPtr,
}

impl PipelineProperties {
    pub fn new(
        arrow_schema: &SchemaRef,
        writer_properties: Option<WriterProperties>,
    ) -> Result<Self> {
        let arrow_schema = Arc::clone(arrow_schema);

        let num_workers = var_os("AMP_PARQUET_WRITER_PARALLELISM")
            .map(|val| val.into_string().map_err(parse_env_var_to_parquet_err))
            .transpose()?
            .map(|s| s.parse::<usize>().map_err(parse_err_to_parquet_err))
            .unwrap_or_else(|| {
                available_parallelism()
                    .map(NonZeroUsize::get)
                    .map_err(ParquetError::from)
            })?
            .max(arrow_schema.flattened_fields().len());

        let channel_capacity = 2 * num_workers;

        let converter = ArrowSchemaConverter::new().with_coerce_types(true);
        let parquet_schema = converter.convert(&arrow_schema).map(Arc::new)?;

        let writer_properties = writer_properties
            .map(add_arrow_schema_to_props(&arrow_schema))
            .unwrap_or_else(default_props_with_arrow_schema(&arrow_schema));

        Ok(Self {
            num_workers,
            arrow_schema,
            parquet_schema,
            channel_capacity,
            writer_properties,
        })
    }

    pub fn with_num_workers(&mut self, num_workers: usize) {
        self.num_workers = num_workers;
        self.channel_capacity = 2 * num_workers;
    }

    pub fn create_encoder_channel(&self) -> (WriterOutbox, EncoderInbox) {
        flume::bounded(self.channel_capacity)
    }

    pub fn create_writer_channel(&self) -> (EncoderOutbox, WriterInbox) {
        flume::unbounded()
    }
}

fn add_arrow_schema_to_props(
    arrow_schema: &SchemaRef,
) -> impl FnOnce(WriterProperties) -> WriterPropertiesPtr {
    move |mut props: WriterProperties| {
        add_encoded_arrow_schema_to_metadata(arrow_schema, &mut props);
        Arc::new(props)
    }
}

fn default_props_with_arrow_schema(
    arrow_schema: &SchemaRef,
) -> impl FnOnce() -> WriterPropertiesPtr {
    || {
        let mut props = WriterProperties::default();
        add_encoded_arrow_schema_to_metadata(arrow_schema, &mut props);
        Arc::new(props)
    }
}

fn parse_err_to_parquet_err(e: std::num::ParseIntError) -> ParquetError {
    ParquetError::External(format!("Failed to parse parallelism value: {e}").into())
}

fn parse_env_var_to_parquet_err(e: std::ffi::OsString) -> ParquetError {
    ParquetError::External(format!("Failed to read parallelism env var: {e:?}").into())
}
