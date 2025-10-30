use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_flight::FlightData;
use common::arrow::{
    array::{ArrayRef, RecordBatch},
    buffer::Buffer,
    datatypes::SchemaRef,
    error::ArrowError,
    ipc::{self, MessageHeader, MetadataVersion, reader},
};
use futures::{Stream, ready};

use crate::{client::ResponseBatch, error::Error};

pub struct FlightDataDecoder<S> {
    flight_data: S,
    state: FlightStreamState,
}

impl<S> FlightDataDecoder<S>
where
    S: Stream<Item = Result<FlightData, tonic::Status>>,
{
    pub fn new(flight_data: S) -> Self {
        Self {
            flight_data,
            state: FlightStreamState::new(),
        }
    }
}

impl<S> Stream for FlightDataDecoder<S>
where
    S: Stream<Item = Result<FlightData, tonic::Status>> + Unpin,
{
    type Item = Result<ResponseBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(Pin::new(&mut self.flight_data).poll_next(cx)) {
                Some(Ok(flight_data)) => match self.state.decode_flight_data(flight_data) {
                    Ok(Some(decoded)) => {
                        let metadata = if decoded.metadata.is_empty() {
                            Default::default()
                        } else {
                            match serde_json::from_slice(&decoded.metadata) {
                                Ok(metadata) => metadata,
                                Err(err) => return Poll::Ready(Some(Err(err.into()))),
                            }
                        };
                        let data = decoded.data;
                        return Poll::Ready(Some(Ok(ResponseBatch { data, metadata })));
                    }
                    Ok(None) => continue,
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                },
                Some(Err(status)) => return Poll::Ready(Some(Err(status.into()))),
                None => return Poll::Ready(None),
            }
        }
    }
}

#[derive(Debug, Clone)]
struct DecodedRecordBatch {
    data: RecordBatch,
    metadata: bytes::Bytes,
}

/// Internal state for decoding FlightData streams
struct FlightStreamState {
    schema: Option<SchemaRef>,
    dictionaries: HashMap<i64, ArrayRef>,
}

impl FlightStreamState {
    fn new() -> Self {
        Self {
            schema: None,
            dictionaries: HashMap::new(),
        }
    }

    fn reset(&mut self, schema: SchemaRef) {
        self.schema = Some(schema);
        self.dictionaries.clear();
    }

    fn decode_flight_data(
        &mut self,
        flight_data: FlightData,
    ) -> Result<Option<DecodedRecordBatch>, ArrowError> {
        if flight_data.data_header.is_empty() {
            return Ok(None);
        }

        let message = ipc::root_as_message(&flight_data.data_header)
            .map_err(|err| ArrowError::ParseError(format!("Failed to parse message: {err}")))?;
        let data = match message.header_type() {
            MessageHeader::RecordBatch => {
                let ipc_record_batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::ParseError("Unable to read IPC message as record batch".to_string())
                })?;
                let schema = self.schema.as_ref().ok_or_else(|| {
                    ArrowError::ParseError("Must receive schema before record batch".to_string())
                })?;
                let buffer = Buffer::from(flight_data.data_body.clone());
                reader::read_record_batch(
                    &buffer,
                    ipc_record_batch,
                    schema.clone(),
                    &self.dictionaries,
                    None,
                    &MetadataVersion::V5,
                )?
            }
            MessageHeader::Schema => {
                let schema = message
                    .header_as_schema()
                    .map(ipc::convert::fb_to_schema)
                    .ok_or_else(|| {
                        ArrowError::ParseError("Unable to read IPC message as schema".to_string())
                    })?;
                self.reset(Arc::new(schema));
                return Ok(None);
            }
            MessageHeader::DictionaryBatch => {
                let ipc_dictionary_batch =
                    message.header_as_dictionary_batch().ok_or_else(|| {
                        ArrowError::ParseError(
                            "Unable to read IPC message as dictionary batch".to_string(),
                        )
                    })?;
                let buffer = Buffer::from(flight_data.data_body.clone());
                reader::read_dictionary(
                    &buffer,
                    ipc_dictionary_batch,
                    self.schema.as_ref().ok_or_else(|| {
                        ArrowError::ParseError("Must receive schema before dictionary".to_string())
                    })?,
                    &mut self.dictionaries,
                    &MetadataVersion::V5,
                )?;
                return Ok(None);
            }
            header_type => {
                return Err(ArrowError::ParseError(format!(
                    "Unsupported message header type: {:?}",
                    header_type
                )));
            }
        };
        let metadata = flight_data.app_metadata;
        Ok(Some(DecodedRecordBatch { data, metadata }))
    }
}
