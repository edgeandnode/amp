use std::{io::Write, marker::PhantomData, mem::MaybeUninit, sync::Arc};

use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::{
    arrow::async_writer::AsyncFileWriter,
    errors::Result,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
};

use crate::{
    builder::{BuildState, Set, Unset},
    writer::pipeline::{PipelineProperties, Progress},
};

pub struct PipelineFactoryBuilder<
    Buf: Default + Write + Send,
    Prog: BuildState = Unset,
    Props: BuildState = Unset,
    Writer: BuildState = Unset,
> where
    Bytes: From<Buf>,
{
    pub(super) progress: MaybeUninit<Option<Progress>>,
    pub(super) properties: MaybeUninit<PipelineProperties>,
    pub(super) file_writer: MaybeUninit<SerializedFileWriter<Buf>>,
    pub(super) _writer_state: PhantomData<Writer>,
    pub(super) _progress_state: PhantomData<Prog>,
    pub(super) _properties_state: PhantomData<Props>,
}

impl<Buf: Default + Write + Send> Default for PipelineFactoryBuilder<Buf>
where
    Bytes: From<Buf>,
{
    fn default() -> Self {
        Self {
            progress: MaybeUninit::uninit(),
            properties: MaybeUninit::uninit(),
            file_writer: MaybeUninit::uninit(),
            _writer_state: PhantomData,
            _progress_state: PhantomData,
            _properties_state: PhantomData,
        }
    }
}

impl<Buf: Default + Write + Send, Props: BuildState, Writer: BuildState>
    PipelineFactoryBuilder<Buf, Unset, Props, Writer>
where
    Bytes: From<Buf>,
{
    pub fn with_progress(mut self) -> PipelineFactoryBuilder<Buf, Set, Props, Writer> {
        self.progress.write(Some(Progress::new()));
        PipelineFactoryBuilder {
            progress: self.progress,
            properties: self.properties,
            file_writer: self.file_writer,
            _writer_state: PhantomData,
            _progress_state: PhantomData,
            _properties_state: PhantomData,
        }
    }

    pub fn without_progress(mut self) -> PipelineFactoryBuilder<Buf, Set, Props, Writer> {
        self.progress.write(None);
        PipelineFactoryBuilder {
            progress: self.progress,
            properties: self.properties,
            file_writer: self.file_writer,
            _writer_state: PhantomData,
            _progress_state: PhantomData,
            _properties_state: PhantomData,
        }
    }
}

impl<Buf: Default + Write + Send, Prog: BuildState> PipelineFactoryBuilder<Buf, Prog, Unset, Unset>
where
    Bytes: From<Buf>,
{
    pub fn build_properties(
        mut self,
        arrow_schema: &SchemaRef,
        writer_properties: Option<WriterProperties>,
    ) -> Result<PipelineFactoryBuilder<Buf, Prog, Set, Unset>> {
        let properties_val = PipelineProperties::new(arrow_schema, writer_properties)?;

        self.properties.write(properties_val);

        Ok(PipelineFactoryBuilder {
            progress: self.progress,
            properties: self.properties,
            file_writer: self.file_writer,
            _writer_state: PhantomData,
            _progress_state: PhantomData,
            _properties_state: PhantomData,
        })
    }
}

impl<Buf: Default + Write + Send, Prog: BuildState> PipelineFactoryBuilder<Buf, Prog, Set, Unset>
where
    Bytes: From<Buf>,
{
    pub fn build_writer(mut self, buf: Buf) -> Result<PipelineFactoryBuilder<Buf, Prog, Set, Set>> {
        let pipeline_properties = unsafe { self.properties.assume_init_ref() };

        let schema_ptr = pipeline_properties.parquet_schema.root_schema_ptr();
        let properties = Arc::clone(&pipeline_properties.writer_properties);
        let writer_val = SerializedFileWriter::new(buf, schema_ptr, properties)?;

        self.file_writer.write(writer_val);

        Ok(PipelineFactoryBuilder {
            progress: self.progress,
            properties: self.properties,
            file_writer: self.file_writer,
            _writer_state: PhantomData,
            _progress_state: PhantomData,
            _properties_state: PhantomData,
        })
    }
}

impl<Buf: Default + Write + Send> PipelineFactoryBuilder<Buf, Unset, Set, Set>
where
    Bytes: From<Buf>,
{
    pub fn into_factory<AsyncWriter: AsyncFileWriter + Send>(
        self,
    ) -> super::PipelineFactory<Buf, AsyncWriter> {
        self.without_progress().into_factory()
    }
}
impl<Buf: Default + Write + Send> PipelineFactoryBuilder<Buf, Set, Set, Set>
where
    Bytes: From<Buf>,
{
    pub fn into_factory<AsyncWriter: AsyncFileWriter + Send>(
        self,
    ) -> super::PipelineFactory<Buf, AsyncWriter> {
        let progress = unsafe { self.progress.assume_init() };
        let properties = unsafe { self.properties.assume_init() };

        super::PipelineFactory {
            progress,
            properties,
            file_writer: self.file_writer,
            writer_inbox: MaybeUninit::uninit(),
            writer_executor: MaybeUninit::uninit(),
            encoder_executor: MaybeUninit::uninit(),
            _inbox_state: PhantomData,
            _writer_state: PhantomData,
            _encoder_state: PhantomData,
            _file_writer_state: PhantomData,
        }
    }
}
