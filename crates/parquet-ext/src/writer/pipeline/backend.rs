use tokio::sync::oneshot::channel;

use super::Progress;
use crate::{
    backend::PipelineBackend,
    writer::pipeline::{
        encoder::{Encoder, EncoderExecutor, EncoderFactory},
        job::WriterInbox,
        properties::PipelineProperties,
    },
};

impl PipelineBackend {
    pub fn spawn_encoder(
        properties: &PipelineProperties,
        factory: EncoderFactory,
        progress: Option<&Progress>,
    ) -> (EncoderExecutor, WriterInbox) {
        let num_workers = properties.num_workers;
        let max_rows = properties.writer_properties.max_row_group_size();

        let (writer_outbox, encoder_inbox) = properties.create_encoder_channel();
        let (encoder_outbox, writer_inbox) = properties.create_writer_channel();
        let (signal, shutdown_receiver) = channel();

        let join_handle = std::thread::spawn(move || {
            Encoder::new(encoder_inbox, num_workers, shutdown_receiver).run()
        });

        let backend = PipelineBackend::new(join_handle, signal);

        let encoder_executor = EncoderExecutor::new(
            backend,
            factory,
            max_rows,
            progress.cloned(),
            writer_outbox,
            encoder_outbox,
        );
        (encoder_executor, writer_inbox)
    }
}
