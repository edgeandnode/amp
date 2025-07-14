import { Schema } from "effect"

export class Dependency extends Schema.Class<Dependency>("Dependency")({
  owner: Schema.String,
  name: Schema.String,
  version: Schema.String,
}) {}

export class TableDefinition extends Schema.Class<TableDefinition>(
  "TableDefinition",
)({
  sql: Schema.String,
}) {}

export const DatasetName = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Name",
    description: "the name of the dataset",
    examples: ["uniswap"],
  }),
)

export const DatasetNetwork = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Network",
    description: "the network that this dataset is for",
    examples: ["mainnet"],
  }),
)

export const DatasetVersion = Schema.String.pipe(
  Schema.pattern(/^\d+\.\d+\.\d+$/),
  Schema.annotations({
    title: "Version",
    description: "a semantic version number (e.g. \"4.1.3\")",
    examples: ["1.0.0", "1.0.1", "1.1.0"],
  }),
)

export const DatasetRepository = Schema.URL.pipe(
  Schema.annotations({
    title: "Repository",
    description: "the address of the repository",
    examples: [new URL("https://github.com/foo/bar")],
  }),
)

export const DatasetReadme = Schema.String.pipe(
  Schema.annotations({
    title: "Readme",
    description: "the documentation of the dataset",
  }),
)

export class FunctionSource extends Schema.Class<FunctionSource>(
  "FunctionSource",
)({
  source: Schema.String,
  filename: Schema.String,
}) {}

export class FunctionDefinition extends Schema.Class<FunctionDefinition>(
  "FunctionDefinition",
)({
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export class DatasetDefinition extends Schema.Class<DatasetDefinition>(
  "DatasetDefinition",
)({
  name: DatasetName,
  network: DatasetNetwork,
  version: DatasetVersion,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: TableDefinition,
  }).pipe(Schema.optional),
  functions: Schema.Record({
    key: Schema.String,
    value: FunctionDefinition,
  }).pipe(Schema.optional),
}) {}

export class ArrowField extends Schema.Class<ArrowField>("ArrowField")({
  name: Schema.String,
  type: Schema.Any,
  nullable: Schema.Boolean,
}) {}

export class ArrowSchema extends Schema.Class<ArrowSchema>("ArrowSchema")({
  fields: Schema.Array(ArrowField),
}) {}

export class TableSchema extends Schema.Class<TableSchema>("TableSchema")({
  arrow: ArrowSchema,
}) {}

export class TableInput extends Schema.Class<TableInput>("TableInput")({
  sql: Schema.String,
}) {}

export class Table extends Schema.Class<Table>("Table")({
  input: TableInput,
  schema: TableSchema,
  network: Schema.String,
}) {}

export class FunctionManifest extends Schema.Class<FunctionManifest>(
  "FunctionManifest",
)({
  name: Schema.String,
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export class DatasetManifest extends Schema.Class<DatasetManifest>(
  "DatasetManifest",
)({
  kind: Schema.Literal("manifest"),
  network: DatasetNetwork,
  name: DatasetName,
  version: DatasetVersion,
  dependencies: Schema.Record({
    key: Schema.String,
    value: Dependency,
  }),
  tables: Schema.Record({
    key: Schema.String,
    value: Table,
  }),
  functions: Schema.Record({
    key: Schema.String,
    value: FunctionManifest,
  }),
}) {}

export class QueryableEvent extends Schema.Class<QueryableEvent>(
  "Nozzle/models/events/QueryableEvent",
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
  source: Schema.NonEmptyTrimmedString.annotations({
    identifier: "QueryableEvent.source",
    description: "Smart Contract source where the event comes from",
    examples: ["contracts/src/Counter.sol"],
  }),
}) {}
export class QueryableEventStream extends Schema.Class<QueryableEventStream>(
  "Nozzle/models/events/QueryableEventStream",
)({
  events: Schema.Array(QueryableEvent),
}) {}
