use arrow_array::RecordBatch;
use flume::{Receiver, Sender};
use parquet::errors::ParquetError;

use super::{EncodedRowGroup, Progress, RowGroupEncoder};

pub type WriterOutbox = Sender<EncodeJob>;
pub type EncoderInbox = Receiver<EncodeJob>;
pub type EncoderOutbox = Sender<WriteJob>;
pub type WriterInbox = Receiver<WriteJob>;

pub struct EncodeJob {
    pub encoder: RowGroupEncoder,
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
    pub finalize: bool,
    pub reply_tx: EncoderOutbox,
    pub progress: Option<Progress>,
}

pub enum WriteJob {
    /// Successfully encoded a row group.
    Encoded { row_group: EncodedRowGroup },
    /// Failed to encode a row group.
    Error { id: usize, error: ParquetError },
    /// Final row group encoded, writer should finalize and reply with metadata.
    Finalize { row_group: EncodedRowGroup },
}
