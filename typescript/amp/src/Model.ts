import * as Schema from "effect/Schema"

export class TableDefinition extends Schema.Class<TableDefinition>(
  "TableDefinition",
)({
  sql: Schema.String,
}) {}

export const DEFAULT_NAMESPACE = "_"

export const DatasetNamespace = Schema.String.pipe(
  Schema.pattern(/^[a-z0-9_]+$/),
  Schema.minLength(1),
  Schema.annotations({
    title: "Namespace",
    description:
      `the namespace/owner of the dataset. If not specified, defaults to "${DEFAULT_NAMESPACE}". Must contain only lowercase letters, digits, and underscores.`,
    examples: ["edgeandnode", "0xdeadbeef", "my_org", "_"],
  }),
)
export type DatasetNamespace = Schema.Schema.Type<typeof DatasetNamespace>

export const DatasetName = Schema.String.pipe(
  Schema.pattern(/^[a-z_][a-z0-9_]*$/),
  Schema.minLength(1),
  Schema.annotations({
    title: "Name",
    description: "the name of the dataset",
    examples: ["uniswap", "eth_mainnet", "_test"],
  }),
)
export type DatasetName = Schema.Schema.Type<typeof DatasetName>

export const Reference = Schema.String.pipe(
  Schema.pattern(/^[a-z0-9_]+\/[a-z_][a-z0-9_]*@.+$/),
  Schema.annotations({
    title: "Reference",
    description: "a dataset reference in the format namespace/name@version",
    examples: ["edgeandnode/mainnet@0.0.0", "0xdeadbeef/eth_firehose@0.0.0"],
  }),
)

/**
 * Parses a Reference to extract the namespace, name, and version components.
 *
 * @param reference - The Reference string to parse in format "namespace/name@version"
 * @returns An object with namespace, name, and version properties
 *
 * @example
 * const { namespace, name, version } = parseReference("edgeandnode/mainnet@1.0.0")
 * // { namespace: "edgeandnode", name: "mainnet", version: "1.0.0" }
 */
export const parseReference = (
  reference: string,
): { namespace: DatasetNamespace; name: DatasetName; version: DatasetVersion } => {
  const atIndex = reference.lastIndexOf("@")
  const slashIndex = reference.indexOf("/")

  // Since the reference passed schema validation, we know it has the correct format
  const namespace = reference.substring(0, slashIndex) as DatasetNamespace
  const name = reference.substring(slashIndex + 1, atIndex) as DatasetName
  const version = reference.substring(atIndex + 1) as DatasetVersion

  return { namespace, name, version }
}

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
export type DatasetVersion = Schema.Schema.Type<typeof DatasetVersion>

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

export class DatasetMetadata extends Schema.Class<DatasetMetadata>(
  "DatasetMetadata",
)({
  namespace: DatasetNamespace,
  name: DatasetName,
  version: DatasetVersion,
}) {}

export class DatasetConfig extends Schema.Class<DatasetConfig>(
  "DatasetConfig",
)({
  namespace: DatasetNamespace.pipe(Schema.optional),
  name: DatasetName,
  network: Network,
  version: DatasetVersion,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  dependencies: Schema.Record({ key: Schema.String, value: Reference }),
  tables: Schema.Record({ key: Schema.String, value: TableDefinition }).pipe(Schema.optional),
  functions: Schema.Record({ key: Schema.String, value: FunctionDefinition }).pipe(Schema.optional),
}) {}

// Legacy alias for backwards compatibility
export const DatasetDefinition = DatasetConfig

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

export class DatasetVersionInfo extends Schema.Class<DatasetVersionInfo>("DatasetVersionInfo")({
  namespace: Schema.String,
  name: DatasetName,
  revision: Schema.String,
  manifestHash: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("manifest_hash")),
  kind: Schema.String,
}) {}

export class DatasetSummary extends Schema.Class<DatasetSummary>("DatasetSummary")({
  namespace: Schema.String,
  name: Schema.String,
  latestVersion: Schema.optional(Schema.String).pipe(Schema.fromKey("latest_version")),
  versions: Schema.Array(Schema.String),
}) {}

export class DatasetsResponse extends Schema.Class<DatasetsResponse>("DatasetsResponse")({
  datasets: Schema.Array(DatasetSummary),
}) {}

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

export const RawDatasetTable = Schema.Struct({
  schema: TableSchema,
  network: Network,
})

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

export class DatasetDerived extends Schema.Class<DatasetDerived>(
  "DatasetDerived",
)({
  kind: Schema.Literal("manifest"),
  namespace: DatasetNamespace.pipe(Schema.optional), // Deprecated: moved to metadata
  name: DatasetName.pipe(Schema.optional), // Deprecated: moved to metadata
  version: DatasetVersion.pipe(Schema.optional), // Deprecated: moved to metadata
  dependencies: Schema.Record({ key: Schema.String, value: Reference }),
  tables: Schema.Record({ key: Schema.String, value: Table }),
  functions: Schema.Record({ key: Schema.String, value: FunctionManifest }),
}) {}

export class DatasetEvmRpc extends Schema.Class<DatasetEvmRpc>("DatasetEvmRpc")({
  kind: Schema.Literal("evm-rpc"),
  namespace: DatasetNamespace.pipe(Schema.optional), // Deprecated: moved to metadata
  name: DatasetName.pipe(Schema.optional), // Deprecated: moved to metadata
  version: DatasetVersion.pipe(Schema.optional), // Deprecated: moved to metadata
  network: Network,
  start_block: Schema.Number.pipe(Schema.optional, Schema.fromKey("start_block")),
  finalized_blocks_only: Schema.Boolean.pipe(Schema.optional, Schema.fromKey("finalized_blocks_only")),
  tables: Schema.Record({ key: Schema.String, value: RawDatasetTable }),
}) {}

/**
 * Union type representing any dataset manifest kind.
 *
 * This type is used at API boundaries (registration, storage, retrieval).
 * Metadata (namespace, name, version) is passed separately to the API.
 *
 * Supported kinds:
 * - DatasetDerived (kind: "manifest") - SQL-based derived datasets
 * - DatasetEvmRpc (kind: "evm-rpc") - EVM RPC extraction datasets
 *
 * Future kinds: DatasetFirehose, DatasetEthBeacon, etc.
 */
export const DatasetManifest = Schema.Union(DatasetDerived, DatasetEvmRpc)
export type DatasetManifest = Schema.Schema.Type<typeof DatasetManifest>

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

export const JobIdParam = Schema.NumberFromString.pipe(
  Schema.annotations({
    title: "JobId",
    description: "unique identifier for a job",
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

export class DeployRequest extends Schema.Class<DeployRequest>("DeployRequest")({
  endBlock: Schema.optional(Schema.NullOr(Schema.String)).pipe(Schema.fromKey("end_block")),
  parallelism: Schema.optional(Schema.Number),
}) {}

export class DeployResponse extends Schema.Class<DeployResponse>("DeployResponse")({
  jobId: JobId.pipe(Schema.propertySignature, Schema.fromKey("job_id")),
}) {}
