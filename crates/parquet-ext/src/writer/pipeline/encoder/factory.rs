use std::{io::Write, sync::Arc};

use arrow_schema::SchemaRef;
use parquet::{
    arrow::arrow_writer::ArrowRowGroupWriterFactory, errors::ParquetError,
    file::writer::SerializedFileWriter,
};

use super::RowGroupEncoder;

pub struct EncoderFactory {
    row_group_index: usize,
    row_group_writer_factory: ArrowRowGroupWriterFactory,
}

impl EncoderFactory {
    pub fn new<W: Write + Send>(
        file_writer: &SerializedFileWriter<W>,
        arrow_schema: &SchemaRef,
    ) -> Self {
        let row_group_index = 0;
        let arrow_schema = Arc::clone(arrow_schema);
        let row_group_writer_factory = ArrowRowGroupWriterFactory::new(file_writer, arrow_schema);

        EncoderFactory {
            row_group_index,
            row_group_writer_factory,
        }
    }

    pub fn try_next_encoder(&mut self) -> Result<RowGroupEncoder, ParquetError> {
        RowGroupEncoder::try_new(self.row_group_index, &self.row_group_writer_factory)
            .inspect(|_| self.row_group_index += 1)
    }
}
