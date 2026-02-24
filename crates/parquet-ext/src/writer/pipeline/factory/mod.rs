mod builder;

use std::{io::Write, marker::PhantomData, mem::MaybeUninit};

use bytes::Bytes;
use parquet::{arrow::async_writer::AsyncFileWriter, file::writer::SerializedFileWriter};

pub use self::builder::PipelineFactoryBuilder;
use super::{EncoderExecutor, PipelineProperties, Progress, WriterExecutor, WriterInbox};
use crate::{
    backend::PipelineBackend,
    builder::{BuildState, Set, Unset},
    writer::pipeline::{Pipeline, encoder::EncoderFactory},
};

pub struct PipelineFactory<
    Buf: Default + Write + Send,
    AsyncWriter: AsyncFileWriter + Send,
    Inbox: BuildState = Unset,
    Encode: BuildState = Unset,
    Writer: BuildState = Unset,
    FileWriter: BuildState = Set,
> where
    Bytes: From<Buf>,
{
    progress: Option<Progress>,
    properties: PipelineProperties,
    file_writer: MaybeUninit<SerializedFileWriter<Buf>>,
    writer_inbox: MaybeUninit<WriterInbox>,
    writer_executor: MaybeUninit<WriterExecutor<AsyncWriter, Buf>>,
    encoder_executor: MaybeUninit<EncoderExecutor>,
    _inbox_state: PhantomData<Inbox>,
    _writer_state: PhantomData<Writer>,
    _encoder_state: PhantomData<Encode>,
    _file_writer_state: PhantomData<FileWriter>,
}

impl<Buf: Default + Write + Send, AsyncWriter: AsyncFileWriter + Send, Writer: BuildState>
    PipelineFactory<Buf, AsyncWriter, Unset, Unset, Writer, Set>
where
    Bytes: From<Buf>,
{
    pub fn spawn_encoder(mut self) -> PipelineFactory<Buf, AsyncWriter, Set, Set, Writer, Set> {
        let file_writer = unsafe { self.file_writer.assume_init_ref() };
        let encoder_factory = EncoderFactory::new(file_writer, &self.properties.arrow_schema);

        let (encoder_executor_val, writer_inbox_val) = PipelineBackend::spawn_encoder(
            &self.properties,
            encoder_factory,
            self.progress.as_ref(),
        );

        self.encoder_executor.write(encoder_executor_val);
        self.writer_inbox.write(writer_inbox_val);

        PipelineFactory {
            progress: self.progress,
            properties: self.properties,
            file_writer: self.file_writer,
            writer_inbox: self.writer_inbox,
            writer_executor: self.writer_executor,
            encoder_executor: self.encoder_executor,
            _inbox_state: PhantomData,
            _writer_state: PhantomData,
            _encoder_state: PhantomData,
            _file_writer_state: PhantomData,
        }
    }
}

impl<Buf: Default + Write + Send, AsyncWriter: AsyncFileWriter + Send>
    PipelineFactory<Buf, AsyncWriter, Set, Set, Unset, Set>
where
    Bytes: From<Buf>,
{
    pub fn spawn_writer(
        mut self,
        writer: AsyncWriter,
    ) -> PipelineFactory<Buf, AsyncWriter, Unset, Set, Set, Unset> {
        let inbox = unsafe { self.writer_inbox.assume_init_read() };
        let file_writer = unsafe { self.file_writer.assume_init_read() };
        let progress = self.progress.as_ref();
        let writer_executor_val = WriterExecutor::new(inbox, writer, file_writer, progress);
        self.writer_executor.write(writer_executor_val);

        PipelineFactory {
            progress: self.progress,
            properties: self.properties,
            file_writer: MaybeUninit::uninit(), // file_writer is moved into writer_executor, so we can't keep it here
            writer_inbox: MaybeUninit::uninit(), // writer_inbox is moved into writer_executor, so we can't keep it here
            writer_executor: self.writer_executor,
            encoder_executor: self.encoder_executor,
            _inbox_state: PhantomData,
            _writer_state: PhantomData,
            _encoder_state: PhantomData,
            _file_writer_state: PhantomData,
        }
    }
}

impl<Buf: Default + Write + Send, AsyncWriter: AsyncFileWriter + Send>
    PipelineFactory<Buf, AsyncWriter, Unset, Set, Set, Unset>
where
    Bytes: From<Buf>,
{
    pub fn build(mut self) -> Pipeline<Buf, AsyncWriter> {
        let encoder_executor = unsafe { self.encoder_executor.assume_init_read() };
        let writer_executor = unsafe { self.writer_executor.assume_init_read() };
        let progress = self.progress.take();

        Pipeline {
            progress,
            encoder_executor,
            writer_executor,
        }
    }
}
