import * as Schema from "effect/Schema"

export class QueryableEvent extends Schema.Class<QueryableEvent>(
  "Nozzle/studio/models/QueryableEvent",
)({
  name: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.name",
    description: "Parsed event name",
    examples: ["Count", "Transfer"],
  }),
  params: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyTrimmedString.annotations({
        identifier: "QueryableEvent.params.name",
        description: "Name of the emitted event param",
      }),
      datatype: Schema.NonEmptyTrimmedString.annotations({
        identifier: "QueryableEvent.params.datatype",
        description: "Type of the emitted event param",
        examples: ["uint256", "bytes32", "address"],
      }),
      indexed: Schema.NullOr(Schema.Boolean).annotations({
        identifier: "QueryableEvent.params.indexed",
        description: "If true, the emitted parameter is indexed",
      }),
    }),
  ).annotations({
    identifier: "QueryableEvent.params",
    description: "The parameters emitted with the event",
    examples: [
      [{ name: "count", datatype: "uint256", indexed: false }],
      [
        {
          name: "from",
          datatype: "address",
          indexed: true,
        },
        {
          name: "to",
          datatype: "address",
          indexed: true,
        },
        {
          name: "value",
          datatype: "uint256",
          indexed: null,
        },
      ],
    ],
  }),
  signature: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.signature",
    description: "The event signature, including the event params.",
    examples: [
      "Count(uint256 count)",
      "Transfer(address indexed from, address indexed to, uint256 value)",
    ],
  }),
  source: Schema.Array(
    Schema.NonEmptyTrimmedString.annotations({
      identifier: "QueryableEvent.source",
      description: "Smart Contract source where the event comes from",
      examples: ["contracts/src/Counter.sol"],
    }),
  ).pipe(
    Schema.minItems(1),
  ),
}) {}
export class QueryableEventStream extends Schema.Class<QueryableEventStream>(
  "Nozzle/studio/models/QueryableEventStream",
)({
  events: Schema.Array(QueryableEvent),
}) {}

export class DatasetMetadata extends Schema.Class<DatasetMetadata>("Nozzle/studio/models/DatasetMetadata")({
  metadata_columns: Schema.Array(Schema.Struct({
    name: Schema.NonEmptyTrimmedString,
    description: Schema.NullishOr(Schema.NonEmptyTrimmedString),
    dataType: Schema.Union(
      Schema.Literal("address"),
      Schema.Literal("bigint"),
      Schema.Literal("string"),
      Schema.Literal("number"),
      Schema.Literal("binary"),
      Schema.Literal("timestamp"),
      Schema.Literal("boolean")
    ),
  })).annotations({
    identifier: "QueryableEventStream.metadata_columns",
    description:
      "Default columns that come with the event source and are availabe on every table to query. They are data points parsed from the EVM call logs",
    examples: [
      [
        { name: "address", description: "The 0x address that invoked the transaction", dataType: "address" },
        { name: "block_num", description: "The block # when the transaction occurred", dataType: "bigint" },
        {
          name: "timestamp",
          description: "The timestamp, in unix-seconds, when the transaction occurred",
          dataType: "bigint",
        },
      ],
    ],
  }),
  source: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEventStream.source",
    description: "Defines the queryable source of the data. Ex: for foundry events, this is the anvil logs.",
    examples: ["anvil.logs", "erc20token.transfers"],
  }),
}) {}

// Arrow type definitions
export interface ArrowField {
  name: string
  type: string | { [key: string]: any }
  nullable: boolean
}

// Helper function to map Arrow types to simplified types
export const mapArrowTypeToSimpleType = (field: ArrowField): "address" | "bigint" | "string" | "number" | "binary" | "timestamp" | "boolean" => {
  const type = field.type
  
  if (typeof type === 'string') {
    switch (type) {
      case 'UInt64':
      case 'UInt32':
      case 'UInt16':
      case 'UInt8':
      case 'Int64':
      case 'Int32':
      case 'Int16':
      case 'Int8':
      case 'Float32':
      case 'Float64':
        return 'number'
      case 'Utf8':
      case 'LargeUtf8':
        return 'string'
      case 'Boolean':
        return 'boolean'
      default:
        return 'string' // fallback
    }
  }
  
  if (typeof type === 'object') {
    if ('FixedSizeBinary' in type) {
      if (type.FixedSizeBinary === 20) {
        return 'address'
      }
      return 'binary'
    }
    if ('Timestamp' in type) {
      return 'timestamp'
    }
  }
  
  return 'string' // ultimate fallback
}
