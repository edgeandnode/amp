//! Arrow to PostgreSQL Binary Format Encoder
//!
//! This crate provides high-performance encoding of Apache Arrow [`RecordBatch`] data
//! into PostgreSQL's COPY binary format, enabling efficient bulk data loading.
//!
//! # Features
//!
//! - **Automatic type mapping**: Maps Arrow types to appropriate PostgreSQL types
//! - **Custom encoders**: Support for custom type mappings (e.g., String → JSONB)
//!
//! # Quick Start
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use arrow_array::{Int32Array, RecordBatch, StringArray};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create an Arrow RecordBatch
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]));
//!
//! let batch = RecordBatch::try_new(
//!     schema.clone(),
//!     vec![
//!         Arc::new(Int32Array::from(vec![1, 2, 3])),
//!         Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
//!     ],
//! )?;
//!
//! // Encode to PostgreSQL binary format
//! let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema)?;
//! let (buffer, _finished) = encoder.encode_batch(&batch)?;
//!
//! // `buffer` is now ready to send to PostgreSQL via COPY
//! # Ok(())
//! # }
//! ```
//!
//! # Custom Encoders
//!
//! You can specify custom encoders for specific fields. For example, to encode
//! a string field as JSONB instead of TEXT:
//!
//! ```rust
//! use std::{collections::HashMap, sync::Arc};
//!
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_to_postgres::{
//!     ArrowToPostgresBinaryEncoder,
//!     encoders::{EncoderBuilder, StringEncoderBuilder},
//!     pg_schema_mapper::PostgresType,
//! };
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("metadata", DataType::Utf8, false),
//! ]);
//!
//! // Create custom encoder for the metadata field
//! let mut encoders = HashMap::new();
//! encoders.insert(
//!     "id".to_string(),
//!     EncoderBuilder::try_new(Arc::new(Field::new("id", DataType::Int32, false)))?,
//! );
//! encoders.insert(
//!     "metadata".to_string(),
//!     EncoderBuilder::String(StringEncoderBuilder::new_with_output(
//!         Arc::new(Field::new("metadata", DataType::Utf8, false)),
//!         PostgresType::Jsonb,
//!     )?),
//! );
//!
//! let encoder = ArrowToPostgresBinaryEncoder::try_new_with_encoders(&schema, encoders)?;
//! # Ok(())
//! # }
//! ```

use std::{collections::HashMap, marker::PhantomData};

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema};
use bytes::{BufMut, BytesMut};

use crate::{
    encoders::{BuildEncoder, Encode, EncoderBuilder},
    error::Error,
    pg_schema_mapper::PostgresSchema,
};

pub mod encoders;
pub mod error;
pub mod pg_schema_mapper;

const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Type-state marker: Encoder is ready to encode data.
/// This is the initial state of a newly created encoder.
#[derive(Debug)]
pub struct Ready;

/// Type-state marker: Encoder has finished encoding.
///
/// This state is returned after successfully encoding a batch.
/// The encoder can still be queried for schema information.
#[derive(Debug)]
pub struct Finished;

/// Arrow to PostgreSQL binary format encoder with compile-time state tracking.
///
/// Uses the type-state pattern to ensure correct usage at compile time:
/// - Can only encode once (encode_batch consumes the Ready encoder)
///
/// # Type Parameter
///
/// - `S`: The current state of the encoder ([`Ready`] or [`Finished`])
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
///
/// use arrow_array::{Int32Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
///
/// let batch = RecordBatch::try_new(
///     schema.clone(),
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
/// )?;
///
/// // Create encoder in Ready state
/// let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema)?;
///
/// // Encode consumes the encoder and returns Finished state
/// let (buffer, finished_encoder) = encoder.encode_batch(&batch)?;
///
/// // Can still query schema from finished encoder
/// let pg_schema = finished_encoder.schema();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder<S = Ready> {
    fields: Fields,
    encoder_builders: Vec<EncoderBuilder>,
    _state: PhantomData<S>,
}

pub fn build_encoders(fields: &Fields) -> Vec<(String, Result<EncoderBuilder, Error>)> {
    fields
        .iter()
        .map(|f| (f.name().clone(), EncoderBuilder::try_new(f.clone())))
        .collect()
}

// Methods available in Ready state
impl ArrowToPostgresBinaryEncoder<Ready> {
    /// Creates a new encoder ready to encode data.
    ///
    /// The encoder will automatically infer PostgreSQL types from the Arrow schema
    /// using sensible defaults (e.g., Arrow Utf8 → PostgreSQL TEXT).
    ///
    /// # Errors
    ///
    /// Returns an error if any Arrow type cannot be mapped to a PostgreSQL type.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new("name", DataType::Utf8, true),
    ///     Field::new("balance", DataType::Decimal128(38, 18), false),
    /// ]);
    ///
    /// let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema)?;
    /// // encoder is now ready to encode batches with this schema
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(schema: &Schema) -> Result<Self, Error> {
        let fields = schema.fields();

        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, Error> = build_encoders(fields)
            .into_iter()
            .map(|(_, maybe_encoder)| maybe_encoder)
            .collect();

        Ok(ArrowToPostgresBinaryEncoder {
            fields: fields.clone(),
            encoder_builders: maybe_encoder_builders?,
            _state: PhantomData,
        })
    }

    /// Creates a new encoder with custom encoder configuration.
    ///
    /// This allows you to specify custom encoders for specific fields,
    /// such as encoding a string field as JSONB instead of TEXT.
    ///
    /// # Errors
    ///
    /// - Returns [`Error::EncoderMissing`] if the schema contains fields not present in `encoders`
    /// - Returns [`Error::UnknownFields`] if `encoders` contains fields not in the schema
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::{collections::HashMap, sync::Arc};
    ///
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_to_postgres::{
    ///     ArrowToPostgresBinaryEncoder,
    ///     encoders::{EncoderBuilder, StringEncoderBuilder},
    ///     pg_schema_mapper::PostgresType,
    /// };
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("data", DataType::Utf8, false),
    /// ]);
    ///
    /// let mut encoders = HashMap::new();
    ///
    /// // Use default encoder for id
    /// encoders.insert(
    ///     "id".to_string(),
    ///     EncoderBuilder::try_new(Arc::new(Field::new("id", DataType::Int32, false)))?,
    /// );
    ///
    /// // Use JSONB encoder for data field
    /// encoders.insert(
    ///     "data".to_string(),
    ///     EncoderBuilder::String(StringEncoderBuilder::new_with_output(
    ///         Arc::new(Field::new("data", DataType::Utf8, false)),
    ///         PostgresType::Jsonb,
    ///     )?),
    /// );
    ///
    /// let encoder = ArrowToPostgresBinaryEncoder::try_new_with_encoders(&schema, encoders)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new_with_encoders(
        schema: &Schema,
        mut encoders: HashMap<String, EncoderBuilder>,
    ) -> Result<Self, Error> {
        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, Error> = schema
            .fields()
            .iter()
            .map(|f| {
                encoders.remove(f.name()).map_or_else(
                    || {
                        Err(Error::EncoderMissing {
                            field: f.name().to_string(),
                        })
                    },
                    Ok,
                )
            })
            .collect();
        if !encoders.is_empty() {
            return Err(Error::UnknownFields {
                fields: encoders.keys().cloned().collect(),
            });
        }
        Ok(ArrowToPostgresBinaryEncoder {
            fields: schema.fields.clone(),
            encoder_builders: maybe_encoder_builders?,
            _state: PhantomData,
        })
    }

    /// Encode a RecordBatch to PostgreSQL binary format.
    ///
    /// Consumes the encoder (can only encode once) and returns:
    /// - The encoded binary data ready for PostgreSQL COPY
    /// - A [`Finished`] encoder (which can still be queried for schema)
    ///
    /// # Type Safety
    ///
    /// This method can only be called once due to consuming `self`.
    /// Attempting to encode twice will be a compile-time error.
    ///
    /// # Performance
    ///
    /// - Creates encoders once and reuses them for both size calculation and encoding
    /// - Pre-allocates exact buffer capacity to avoid reallocations
    /// - Optimized for single-pass encoding with zero unnecessary copies
    ///
    /// # Errors
    ///
    /// - Returns [`Error::ColumnCountMismatch`] if the batch has different columns than the schema
    /// - Returns [`Error::InvalidJson`] if a JSONB field contains invalid JSON
    /// - Returns encoder-specific errors for unsupported type combinations
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use arrow_array::{Int32Array, RecordBatch, StringArray};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("name", DataType::Utf8, false),
    /// ]));
    ///
    /// let batch = RecordBatch::try_new(
    ///     schema.clone(),
    ///     vec![
    ///         Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///         Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
    ///     ],
    /// )?;
    ///
    /// let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema)?;
    /// let (buffer, _finished) = encoder.encode_batch(&batch)?;
    ///
    /// // buffer contains PostgreSQL COPY binary format data
    /// // Use it with PostgreSQL's COPY command:
    /// // COPY table_name (id, name) FROM STDIN WITH (FORMAT BINARY)
    /// # Ok(())
    /// # }
    /// ```
    pub fn encode_batch(
        self,
        batch: &RecordBatch,
    ) -> Result<(BytesMut, ArrowToPostgresBinaryEncoder<Finished>), Error> {
        // Validate columns
        if batch.num_columns() != self.fields.len() {
            return Err(Error::column_count_mismatch(
                self.fields.len(),
                batch.num_columns(),
            ));
        }

        // Create encoders ONCE - this is the key performance optimization
        let encoders = batch
            .columns()
            .iter()
            .zip(&self.encoder_builders)
            .map(|(col, builder)| builder.try_new(col))
            .collect::<Result<Vec<_>, _>>()?;

        // Calculate size using the encoders we just created
        let mut data_size: usize = 0;
        for encoder in &encoders {
            data_size += encoder.size_hint()?;
        }

        // Add row overhead: each row has a 2-byte column count
        let n_rows = batch.num_rows();
        data_size += n_rows * 2;

        // Header size: magic bytes (12) + flags (4) + extension (4) = 20 bytes
        const HEADER_SIZE: usize = 20;
        // Footer size: -1 as i16 = 2 bytes
        const FOOTER_SIZE: usize = 2;

        let buffer_size = HEADER_SIZE + data_size + FOOTER_SIZE;

        // Allocate buffer with exact capacity
        let mut buffer = BytesMut::with_capacity(buffer_size);

        // Write header
        buffer.put(HEADER_MAGIC_BYTES);
        buffer.put_i32(0); // flags
        buffer.put_i32(0); // header extension

        // Write batch data using the encoders we created earlier
        let n_cols = batch.num_columns();
        for row in 0..n_rows {
            buffer.put_i16(n_cols as i16);
            for encoder in &encoders {
                encoder.encode(row, &mut buffer)?;
            }
        }

        // Write footer
        buffer.put_i16(-1);

        // Transition to Finished state
        let finished_encoder = ArrowToPostgresBinaryEncoder {
            fields: self.fields,
            encoder_builders: self.encoder_builders,
            _state: PhantomData,
        };

        Ok((buffer, finished_encoder))
    }
}

// Methods available in all states
impl<S> ArrowToPostgresBinaryEncoder<S> {
    /// Get the PostgreSQL schema for this encoder.
    ///
    /// Returns a [`PostgresSchema`] describing the PostgreSQL column types and nullability
    /// that this encoder will produce. This is useful for generating CREATE TABLE statements
    /// or validating that your schema matches an existing table.
    ///
    /// This method is available regardless of encoder state ([`Ready`] or [`Finished`]).
    ///
    /// # Example
    ///
    /// ```rust
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new("name", DataType::Utf8, true),
    /// ]);
    ///
    /// let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema)?;
    /// let pg_schema = encoder.schema();
    ///
    /// // pg_schema.columns contains:
    /// // - ("id", Column { data_type: PostgresType::Int8, nullable: false })
    /// // - ("name", Column { data_type: PostgresType::Text, nullable: true })
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> PostgresSchema {
        PostgresSchema {
            columns: self
                .encoder_builders
                .iter()
                .zip(&self.fields)
                .map(|(builder, field)| (field.name().clone(), builder.schema()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{Int8Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::{
        encoders::StringEncoderBuilder,
        pg_schema_mapper::{Column, PostgresType},
    };

    fn make_test_data() -> RecordBatch {
        let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let int8_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
        let string_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let json_array = StringArray::from(vec!["\"a\"", "[]", "{\"f\":123}", "1", "{}"]);

        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("json", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(int32_array),
                Arc::new(int8_array),
                Arc::new(string_array),
                Arc::new(json_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_build_with_encoders() {
        let batch = make_test_data();
        let encoders = build_encoders(batch.schema().fields());
        let encoders: HashMap<String, EncoderBuilder> = encoders
            .into_iter()
            .map(|(field_name, maybe_enc)| match field_name.as_str() {
                "json" => (
                    field_name.to_string(),
                    EncoderBuilder::String(
                        StringEncoderBuilder::new_with_output(
                            Arc::new(batch.schema().field_with_name("json").unwrap().clone()),
                            PostgresType::Jsonb,
                        )
                        .unwrap(),
                    ),
                ),
                field_name => (field_name.to_string(), maybe_enc.unwrap()),
            })
            .collect();
        let encoder = ArrowToPostgresBinaryEncoder::try_new_with_encoders(
            &batch.schema(),
            encoders.into_iter().collect(),
        )
        .unwrap();
        let schema = encoder.schema();
        assert_eq!(
            schema.columns,
            vec![
                (
                    "int32".to_owned(),
                    Column {
                        data_type: PostgresType::Int4,
                        nullable: false,
                    }
                ),
                (
                    "int8".to_owned(),
                    Column {
                        data_type: PostgresType::Int2,
                        nullable: false,
                    },
                ),
                (
                    "string".to_owned(),
                    Column {
                        data_type: PostgresType::Text,
                        nullable: false,
                    },
                ),
                (
                    "json".to_owned(),
                    Column {
                        data_type: PostgresType::Jsonb,
                        nullable: false,
                    },
                ),
            ]
        )
    }

    #[test]
    fn test_encode_batch() {
        // Test the encode_batch method
        let batch = make_test_data();
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&batch.schema()).unwrap();

        // Encode the batch - encoder is consumed and returns Finished encoder
        let (buffer, _finished_encoder) = encoder.encode_batch(&batch).unwrap();

        // Verify we got a valid buffer with header and footer
        assert!(buffer.len() > 20); // At least header size
        assert_eq!(&buffer[0..11], b"PGCOPY\n\xff\r\n\0"); // Magic bytes

        // Note: encoder is consumed, can't use it again
        // This is enforced at compile time!
    }

    #[test]
    fn test_type_state_prevents_double_encode() {
        // This test verifies that the type system prevents encoding twice
        let batch = make_test_data();
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&batch.schema()).unwrap();

        // First encode succeeds and consumes encoder
        let (_buffer, _finished) = encoder.encode_batch(&batch).unwrap();

        // Attempting to encode again would be a compile error:
        // let (_buffer2, _finished2) = encoder.encode_batch(&batch).unwrap();
        // Error: use of moved value: `encoder`

        // This is exactly what we want - compile-time prevention of misuse!
    }

    #[test]
    fn test_schema_available_in_all_states() {
        let batch = make_test_data();
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&batch.schema()).unwrap();

        // Can get schema in Ready state
        let schema1 = encoder.schema();
        assert_eq!(schema1.columns.len(), 4);

        // Encode the batch
        let (_buffer, finished_encoder) = encoder.encode_batch(&batch).unwrap();

        // Can still get schema in Finished state
        let schema2 = finished_encoder.schema();
        assert_eq!(schema2.columns.len(), 4);
        assert_eq!(schema1.columns, schema2.columns);
    }

    #[test]
    fn test_finished_encoder_type() {
        // Verify that encode_batch returns the correct type
        let batch = make_test_data();
        let encoder: ArrowToPostgresBinaryEncoder<Ready> =
            ArrowToPostgresBinaryEncoder::try_new(&batch.schema()).unwrap();

        let (buffer, finished): (BytesMut, ArrowToPostgresBinaryEncoder<Finished>) =
            encoder.encode_batch(&batch).unwrap();

        assert!(!buffer.is_empty());
        // finished is of type ArrowToPostgresBinaryEncoder<Finished>
        let _ = finished.schema(); // Can call methods available to all states
    }
}
