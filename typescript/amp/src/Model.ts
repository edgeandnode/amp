import * as Schema from "effect/Schema"

export class Dependency extends Schema.Class<Dependency>("Dependency")({
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

export const Network = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Network",
    description: "the network of a dataset or provider",
    examples: ["mainnet"],
  }),
)

export const DatasetKind = Schema.Literal("manifest", "sql", "firehose", "evm-rpc").pipe(
  Schema.annotations({
    title: "Kind",
    description: "the kind of dataset",
    examples: ["manifest", "sql", "firehose", "evm-rpc"],
  }),
)

export const DatasetVersion = Schema.String.pipe(
  Schema.pattern(
    /^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/,
  ),
  Schema.annotations({
    title: "Version",
    description: "a semantic version number (e.g. \"4.1.3\")",
    examples: ["1.0.0", "1.0.1", "1.1.0", "1.0.0-dev123", "1.0.0+1234567890"],
  }),
)

export const DatasetNameAndVersion = Schema.TemplateLiteral(Schema.String, Schema.Literal("@"), Schema.String).pipe(
  Schema.pattern(
    /^\w+@(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/,
  ),
  Schema.annotations({
    title: "NameAndVersion",
    description: "the name and version of the dataset",
    examples: ["uniswap@1.0.0", "uniswap@1.0.0+1234567890"],
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
  network: Network,
  version: DatasetVersion,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  dependencies: Schema.Record({ key: Schema.String, value: Dependency }),
  tables: Schema.Record({ key: Schema.String, value: TableDefinition }).pipe(Schema.optional),
  functions: Schema.Record({ key: Schema.String, value: FunctionDefinition }).pipe(Schema.optional),
}) {}

export class TableInfo extends Schema.Class<TableInfo>("TableInfo")({
  name: Schema.String,
  network: Network,
  activeLocation: Schema.String.pipe(Schema.optional, Schema.fromKey("active_location")),
}) {}

export class TableSchemaInfo extends Schema.Class<TableSchemaInfo>("TableSchemaInfo")({
  name: Schema.String,
  network: Network,
  schema: Schema.Record({ key: Schema.String, value: Schema.Any }),
}) {}

export class DatasetInfo extends Schema.Class<DatasetInfo>("DatasetInfo")({
  name: DatasetName,
  kind: DatasetKind,
  tables: Schema.Array(TableInfo),
}) {}

export class DatasetSchemaResponse extends Schema.Class<DatasetSchemaResponse>("DatasetSchemaResponse")({
  name: DatasetName,
  version: DatasetVersion,
  tables: Schema.Array(TableSchemaInfo),
}) {}

export class DatasetRegistryInfo extends Schema.Class<DatasetRegistryInfo>("DatasetRegistryInfo")({
  name: DatasetName,
  version: DatasetVersion,
}) {}

/**
 * Dataset cursor for pagination in "name:version" format (e.g., "eth_mainnet:1.0.0")
 */
export const DatasetCursor = Schema.String.pipe(
  Schema.pattern(/^[^:]+:[^:]+$/),
  Schema.annotations({
    description: "Dataset cursor in 'name:version' format for pagination",
    examples: ["eth_mainnet:1.0.0", "uniswap:2.1.0"],
  }),
  Schema.brand("DatasetCursor"),
)
export type DatasetCursor = Schema.Schema.Type<typeof DatasetCursor>

/**
 * Creates a DatasetCursor from a dataset name and version.
 *
 * @param name - The dataset name
 * @param version - The dataset version
 * @returns A properly formatted DatasetCursor
 *
 * @example
 * const cursor = makeDatasetCursor("eth_mainnet", "1.0.0") // "eth_mainnet:1.0.0"
 */
export const makeDatasetCursor = (name: string, version: string): DatasetCursor => {
  const cursorString = `${name}:${version}`
  // This will validate the format and throw if invalid
  return Schema.decodeSync(DatasetCursor)(cursorString)
}

/**
 * Parses a DatasetCursor to extract the name and version components.
 *
 * @param cursor - The DatasetCursor to parse
 * @returns An object with name and version properties
 *
 * @example
 * const { name, version } = parseDatasetCursor(cursor) // { name: "eth_mainnet", version: "1.0.0" }
 */
export const parseDatasetCursor = (cursor: DatasetCursor): { name: string; version: string } => {
  const parts = cursor.split(":")
  // Since the cursor passed schema validation, we know it has exactly one colon
  const [name, version] = parts
  return { name: name!, version: version! }
}

export class DatasetsResponse extends Schema.Class<DatasetsResponse>("DatasetsResponse")({
  datasets: Schema.Array(DatasetRegistryInfo),
}) {}

/**
 * Dataset version cursor for pagination (e.g., "1.0.0", "2.1.3")
 */
export const DatasetVersionCursor = DatasetVersion.pipe(
  Schema.annotations({
    description: "Dataset version cursor for pagination",
    examples: ["1.0.0", "2.1.3"],
  }),
  Schema.brand("DatasetVersionCursor"),
)
export type DatasetVersionCursor = Schema.Schema.Type<typeof DatasetVersionCursor>

/**
 * Response for listing versions of a specific dataset
 */
export class DatasetVersionsResponse extends Schema.Class<DatasetVersionsResponse>("DatasetVersionsResponse")({
  versions: Schema.Array(DatasetVersion),
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
  network: Network,
}) {}

export class OutputSchema extends Schema.Class<OutputSchema>("OutputSchema")({
  schema: TableSchema,
  networks: Schema.Array(Schema.String),
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
  network: Network,
  name: DatasetName,
  version: DatasetVersion,
  dependencies: Schema.Record({ key: Schema.String, value: Dependency }),
  tables: Schema.Record({ key: Schema.String, value: Table }),
  functions: Schema.Record({ key: Schema.String, value: FunctionManifest }),
}) {}

export class DatasetRpc extends Schema.Class<DatasetRpc>("DatasetRpc")({
  kind: Schema.Literal("evm-rpc"),
  network: Network,
  name: DatasetName,
  version: DatasetVersion,
  schema: Schema.Record({ key: Schema.String, value: Schema.Any }),
}) {}

export class EvmRpcProvider extends Schema.Class<EvmRpcProvider>("EvmRpcProvider")({
  kind: Schema.Literal("evm-rpc"),
  network: Network,
  url: Schema.URL,
}) {}

export const Provider = Schema.Union(EvmRpcProvider).pipe(
  Schema.annotations({
    title: "Provider",
    description: "a provider definition",
  }),
)

export const JobId = Schema.Number.pipe(
  Schema.annotations({
    title: "JobId",
    description: "unique identifier for a job",
  }),
)

export const JobStatus = Schema.Literal("RUNNING", "TERMINAL", "COMPLETED", "STOPPED", "ERROR").pipe(
  Schema.annotations({
    title: "JobStatus",
    description: "the status of a job",
  }),
)

export const JobStatusParam = Schema.Literal("terminal", "complete", "stopped", "error").pipe(
  Schema.annotations({
    title: "JobStatus",
    description: "the status of a job",
  }),
)

export const JobIdParam = Schema.NumberFromString.pipe(
  Schema.annotations({
    title: "JobId",
    description: "unique identifier for a job",
  }),
)

export const LocationId = Schema.Number.pipe(
  Schema.annotations({
    title: "LocationId",
    description: "unique identifier for a location",
  }),
)

export const LocationIdParam = Schema.NumberFromString.pipe(
  Schema.annotations({
    title: "LocationId",
    description: "unique identifier for a location",
  }),
)

export class JobInfo extends Schema.Class<JobInfo>("JobInfo")({
  id: JobId,
  createdAt: Schema.DateTimeUtc.pipe(Schema.propertySignature, Schema.fromKey("created_at")),
  updatedAt: Schema.DateTimeUtc.pipe(Schema.propertySignature, Schema.fromKey("updated_at")),
  nodeId: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("node_id")),
  status: JobStatus,
  descriptor: Schema.Any,
}) {}

export class LocationInfo extends Schema.Class<LocationInfo>("LocationInfo")({
  id: LocationId,
  dataset: Schema.String,
  datasetVersion: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("dataset_version")),
  table: Schema.String,
  url: Schema.String,
  active: Schema.Boolean,
  writer: Schema.optional(JobId),
}) {}

export class JobsResponse extends Schema.Class<JobsResponse>("JobsResponse")({
  jobs: Schema.Array(JobInfo),
  nextCursor: Schema.optional(JobId).pipe(Schema.fromKey("next_cursor")),
}) {}

export class LocationsResponse extends Schema.Class<LocationsResponse>("LocationsResponse")({
  locations: Schema.Array(LocationInfo),
  nextCursor: Schema.optional(LocationId).pipe(Schema.fromKey("next_cursor")),
}) {}

export class BlockRange extends Schema.Class<BlockRange>("BlockRange")({
  network: Schema.String,
  numbers: Schema.Struct({
    start: Schema.Number,
    end: Schema.Number,
  }),
  hash: Schema.String,
  prevHash: Schema.String.pipe(Schema.optional),
}) {}

export class RecordBatchMetadata extends Schema.Class<RecordBatchMetadata>("RecordBatchMetadata")({
  ranges: Schema.Array(BlockRange),
}) {}

export class DumpResponse extends Schema.Class<DumpResponse>("DumpResponse")({
  job_id: JobId,
}) {}
