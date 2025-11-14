import { RecordBatch, RecordBatchReader, RecordBatchWriter } from "apache-arrow"
import * as ParseResult from "effect/ParseResult"
import * as Schema from "effect/Schema"
import { isAddress } from "viem"

export class TableDefinition extends Schema.Class<TableDefinition>(
  "TableDefinition",
)({
  sql: Schema.String,
}) {}

export const Address = Schema.NonEmptyTrimmedString.pipe(Schema.filter((val) => isAddress(val)))

export const Network = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Network",
    description: "the network of a dataset or provider",
    examples: ["eth_mainnet"],
  }),
)

export type DatasetNamespace = typeof DatasetNamespace.Type
export const DatasetNamespace = Schema.String.pipe(
  Schema.pattern(/^[a-z0-9_]+$/),
  Schema.minLength(1),
  Schema.annotations({
    title: "DatasetNamespace",
    description:
      `the namespace/owner of the dataset. If not specified, defaults to "_". Must contain only lowercase letters, digits, and underscores.`,
    examples: ["edgeandnode", "0xdeadbeef", "my_org", "_"],
  }),
  Schema.brand("DatasetNamespace"),
)

export const DEFAULT_NAMESPACE = DatasetNamespace.make("_")

export type DatasetName = typeof DatasetName.Type
export const DatasetName = Schema.String.pipe(
  Schema.pattern(/^[a-z_][a-z0-9_]*$/),
  Schema.minLength(1),
  Schema.annotations({
    title: "DatasetName",
    description:
      "the name of the dataset (must start with lowercase letter or underscore, followed by lowercase letters, digits, or underscores)",
    examples: ["uniswap"],
  }),
  Schema.brand("DatasetName"),
)

export type DatasetKind = typeof DatasetKind.Type
export const DatasetKind = Schema.Literal("manifest", "evm-rpc", "eth-beacon", "firehose").pipe(
  Schema.annotations({
    title: "DatasetKind",
    description: "the kind of dataset",
    examples: ["manifest", "evm-rpc", "eth-beacon", "firehose"],
  }),
  Schema.brand("DatasetKind"),
)

export type DatasetVersion = typeof DatasetVersion.Type
export const DatasetVersion = Schema.String.pipe(
  Schema.pattern(
    /^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/,
  ),
  Schema.annotations({
    title: "DatasetVersion",
    description: "a semantic version number (e.g. \"4.1.3\")",
    examples: ["1.0.0", "1.0.1", "1.1.0", "1.0.0-dev123", "1.0.0+1234567890"],
  }),
  Schema.brand("DatasetVersion"),
)

export type DatasetHash = typeof DatasetHash.Type
export const DatasetHash = Schema.String.pipe(
  Schema.pattern(/^[0-9a-fA-F]{64}$/),
  Schema.length(64),
  Schema.annotations({
    title: "DatasetHash",
    description: "a 32-byte SHA-256 hash (64 hex characters)",
    examples: ["b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"],
  }),
  Schema.brand("DatasetHash"),
)

export type DatasetTag = typeof DatasetTag.Type
export const DatasetTag = Schema.Literal("latest", "dev").pipe(
  Schema.annotations({
    title: "DatasetTag",
    description: "a tag for a dataset version",
    examples: ["latest", "dev"],
  }),
  Schema.brand("DatasetTag"),
)

export type DatasetRevision =
  | DatasetTag
  | DatasetVersion
  | DatasetHash

export const DatasetRevision: Schema.Schema<DatasetRevision, string> = Schema.Union(
  DatasetVersion,
  DatasetHash,
  DatasetTag,
).pipe(
  Schema.annotations({
    title: "DatasetRevision",
    description: "a dataset revision reference (semver tag, 64-char hex hash, 'latest', or 'dev')",
    examples: [
      DatasetVersion.make("1.0.0"),
      DatasetHash.make("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"),
      DatasetTag.make("latest"),
      DatasetTag.make("dev"),
    ],
  }),
)

export type DatasetReferenceString = typeof DatasetReferenceString.Type
export const DatasetReferenceString: Schema.Schema<string, string> = Schema.String.pipe(
  Schema.pattern(/^[a-z0-9_]+\/[a-z_][a-z0-9_]*@.+$/),
  Schema.annotations({
    title: "DatasetReferenceString",
    description: "a dataset reference string in the format namespace/name@revision (version, hash, 'latest', or 'dev')",
    examples: [
      "edgeandnode/mainnet@1.0.0",
      "edgeandnode/mainnet@latest",
      "edgeandnode/mainnet@dev",
      "0xdeadbeef/eth_firehose@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
    ],
  }),
)

export class DatasetReference extends Schema.Class<DatasetReference>("DatasetReference")({
  namespace: DatasetNamespace,
  name: DatasetName,
  revision: DatasetRevision,
}) {}

export const DatasetReferenceFromString: Schema.Schema<DatasetReference, string> = DatasetReferenceString.pipe(
  Schema.transformOrFail(DatasetReference, {
    encode: (value) => ParseResult.succeed(`${value.namespace}/${value.name}@${value.revision}`),
    decode: (value) => {
      const at = value.lastIndexOf("@")
      const slash = value.indexOf("/")

      const namespace = value.substring(0, slash)
      const name = value.substring(slash + 1, at)
      const revision = value.substring(at + 1)

      return ParseResult.decode(DatasetReference)({ namespace, name, revision })
    },
  }),
  Schema.annotations({
    identifier: "DatasetReferenceFromString",
    description: "A dataset reference parsed from a string in format 'namespace/name@revision'",
  }),
)

export const DatasetNameAndVersion: Schema.Schema<string, string> = Schema.String.pipe(
  Schema.pattern(
    /^\w+@(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/,
  ),
  Schema.annotations({
    title: "NameAndVersion",
    description: "the name and version of the dataset",
    examples: ["uniswap@1.0.0", "uniswap@1.0.0+1234567890"],
  }),
)

export const DatasetRepository: Schema.Schema<URL, string> = Schema.URL.pipe(
  Schema.annotations({
    title: "Repository",
    description: "the address of the repository",
    examples: [new URL("https://github.com/foo/bar")],
  }),
)

export const DatasetReadme: Schema.Schema<string, string> = Schema.String.pipe(
  Schema.annotations({
    title: "Readme",
    description: "the documentation of the dataset",
  }),
)

export const DatasetDescription = Schema.String.pipe(
  Schema.annotations({
    title: "Description",
    description: "additional description and details about the dataset",
  }),
)

export const DatasetKeyword = Schema.String.pipe(
  Schema.annotations({
    title: "Keyword",
    description: "keywords, or traits, about the dataset for discoverability and searching",
    examples: ["NFT", "Collectibles", "DeFi", "Transfers"],
  }),
)

export const DatasetSource = Schema.String.pipe(
  Schema.annotations({
    title: "Source",
    description:
      "Source of the dataset data. Like the block or logs table that powers the dataset, or the 0x address of the smart contract being queried",
    examples: ["eth_mainnet_rpc.logs", "arbitrum_one_rpc.blocks", "0xc944e90c64b2c07662a292be6244bdf05cda44a7"],
  }),
)

export const DatasetLicense = Schema.String.pipe(
  Schema.annotations({
    title: "License",
    description: "License covering the dataset",
    examples: ["MIT"],
  }),
)

export class FunctionSource extends Schema.Class<FunctionSource>("FunctionSource")({
  source: Schema.String,
  filename: Schema.String,
}) {}

export class FunctionDefinition extends Schema.Class<FunctionDefinition>("FunctionDefinition")({
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export type DatasetTableMap = typeof DatasetTableMap.Type
export const DatasetTableMap = Schema.Record({ key: Schema.String, value: TableDefinition }).pipe(
  Schema.annotations({
    title: "Table Map",
    description: "a map of table names to their definitions",
  }),
)

export type DatasetDependencyMap = typeof DatasetDependencyMap.Type
export const DatasetDependencyMap = Schema.Record({ key: Schema.String, value: DatasetReferenceFromString }).pipe(
  Schema.annotations({
    title: "Dependency Reference Map",
    description: "a map of dataset names to their references",
  }),
)

export type DatasetFunctionDefinitionMap = typeof DatasetFunctionDefinitionMap.Type
export const DatasetFunctionDefinitionMap = Schema.Record({ key: Schema.String, value: FunctionDefinition }).pipe(
  Schema.annotations({
    title: "Function Definition Map",
    description: "a map of function names to their definitions",
  }),
)

export class DatasetMetadata extends Schema.Class<DatasetMetadata>("DatasetMetadata")({
  namespace: DatasetNamespace,
  name: DatasetName,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  description: DatasetDescription.pipe(Schema.optional),
  keywords: Schema.Array(DatasetKeyword).pipe(Schema.optional),
  sources: Schema.Array(DatasetSource).pipe(Schema.optional),
  license: DatasetLicense.pipe(Schema.optional),
  visibility: Schema.Literal("public", "private").pipe(Schema.optional),
}, {
  title: "Dataset Metadata",
  description: "metadata for a dataset",
}) {}

export class DatasetConfig extends Schema.Class<DatasetConfig>("DatasetConfig")({
  namespace: DatasetNamespace.pipe(Schema.optional),
  name: DatasetName,
  network: Network,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  description: DatasetDescription.pipe(Schema.optional),
  keywords: Schema.Array(DatasetKeyword).pipe(Schema.optional),
  sources: Schema.Array(DatasetSource).pipe(Schema.optional),
  license: DatasetLicense.pipe(Schema.optional),
  private: Schema.Boolean.pipe(Schema.optional),
  dependencies: DatasetDependencyMap,
  tables: DatasetTableMap.pipe(Schema.optional),
  functions: DatasetFunctionDefinitionMap.pipe(Schema.optional),
}, {
  title: "Dataset Configuration",
  description: "configuration for a dataset",
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

export class FunctionManifest extends Schema.Class<FunctionManifest>("FunctionManifest")({
  name: Schema.String,
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String,
}) {}

export class DatasetDerived extends Schema.Class<DatasetDerived>("DatasetDerived")({
  kind: Schema.Literal("manifest"),
  dependencies: DatasetDependencyMap,
  tables: Schema.Record({ key: Schema.String, value: Table }),
  functions: Schema.Record({ key: Schema.String, value: FunctionManifest }),
}) {}

export class DatasetEvmRpc extends Schema.Class<DatasetEvmRpc>("DatasetEvmRpc")({
  kind: Schema.Literal("evm-rpc"),
  network: Network,
  start_block: Schema.Number.pipe(Schema.optional, Schema.fromKey("start_block")),
  finalized_blocks_only: Schema.Boolean.pipe(Schema.optional, Schema.fromKey("finalized_blocks_only")),
  tables: Schema.Record({ key: Schema.String, value: RawDatasetTable }),
}) {}

export class DatasetEthBeacon extends Schema.Class<DatasetEthBeacon>("DatasetEthBeacon")({
  kind: Schema.Literal("eth-beacon"),
  network: Network,
  start_block: Schema.Number.pipe(Schema.optional, Schema.fromKey("start_block")),
  finalized_blocks_only: Schema.Boolean.pipe(Schema.optional, Schema.fromKey("finalized_blocks_only")),
  tables: Schema.Record({ key: Schema.String, value: RawDatasetTable }),
}) {}

export class DatasetFirehose extends Schema.Class<DatasetFirehose>("DatasetFirehose")({
  kind: Schema.Literal("firehose"),
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
 * - DatasetEthBeacon (kind: "eth-beacon") - ETH beacon extraction datasets
 * - DatasetFirehose (kind: "firehose") - Firehose extraction datasets
 */
export const DatasetManifest = Schema.Union(DatasetDerived, DatasetEvmRpc, DatasetEthBeacon, DatasetFirehose)
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

export const JobStatus = Schema.Literal(
  "SCHEDULED",
  "RUNNING",
  "COMPLETED",
  "STOPPED",
  "STOP_REQUESTED",
  "STOPPING",
  "FAILED",
  "UNKNOWN",
).pipe(
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

/**
 * Invalidation range for reorgs.
 */
export class InvalidationRange extends Schema.Class<InvalidationRange>("InvalidationRange")({
  network: Schema.String,
  numbers: Schema.Struct({
    start: Schema.Number,
    end: Schema.Number,
  }),
}) {}

export class RecordBatchMetadata extends Schema.Class<RecordBatchMetadata>("RecordBatchMetadata")({
  ranges: Schema.Array(BlockRange),
  // rangesComplete: Schema.Boolean.pipe(Schema.propertySignature, Schema.fromKey("ranges_complete")),
  rangesComplete: Schema.Boolean.pipe(Schema.propertySignature, Schema.fromKey("ranges_complete")),
}) {}

const decoder = new TextDecoder()
const encoder = new TextEncoder()

export const parseRecordBatchMetadata: <A, R>(
  from: Schema.Schema<Uint8Array, A, R>,
) => Schema.Schema<RecordBatchMetadata, A, R> = Schema.transformOrFail(RecordBatchMetadata, {
  encode: (value, _, ast) =>
    ParseResult.try({
      try: () => encoder.encode(JSON.stringify(value)),
      catch: () => new ParseResult.Type(ast, value, "Failed to encode RecordBatchMetadata"),
    }),
  decode: (value, _, ast) =>
    ParseResult.try({
      try: () => JSON.parse(decoder.decode(value)),
      catch: () => new ParseResult.Type(ast, value, "Failed to decode RecordBatchMetadata"),
    }),
})

export const RecordBatchMetadataFromFlight = parseRecordBatchMetadata(Schema.Uint8ArrayFromSelf)

export class DumpResponse extends Schema.Class<DumpResponse>("DumpResponse")({
  jobId: JobId.pipe(Schema.propertySignature, Schema.fromKey("job_id")),
}) {}

export class DeployRequest extends Schema.Class<DeployRequest>("DeployRequest")({
  endBlock: Schema.optional(Schema.NullOr(Schema.String)).pipe(Schema.fromKey("end_block")),
  parallelism: Schema.optional(Schema.Number),
}) {}

export class DeployResponse extends Schema.Class<DeployResponse>("DeployResponse")({
  jobId: JobId.pipe(Schema.propertySignature, Schema.fromKey("job_id")),
}) {}

export const GenrateTokenDuration = Schema.String.pipe(
  Schema.pattern(
    /^-?\d+\.?\d*\s*(sec|secs|second|seconds|s|minute|minutes|min|mins|m|hour|hours|hr|hrs|h|day|days|d|week|weeks|w|year|years|yr|yrs|y)(\s+ago|\s+from\s+now)?$/i,
  ),
)
export type GenrateTokenDuration = typeof GenrateTokenDuration.Type

export class SchemaRequest extends Schema.Class<SchemaRequest>("SchemaRequest")({
  tables: Schema.Record({ key: Schema.String, value: Schema.String }),
  dependencies: DatasetDependencyMap.pipe(Schema.optional),
  functions: DatasetFunctionDefinitionMap.pipe(Schema.optional),
}) {}

export class TableSchemaWithNetworks extends Schema.Class<TableSchemaWithNetworks>("TableSchemaWithNetworks")({
  schema: TableSchema,
  networks: Schema.Array(Schema.String),
}) {}

export class SchemaResponse extends Schema.Class<SchemaResponse>("SchemaResponse")({
  schemas: Schema.Record({ key: Schema.String, value: TableSchemaWithNetworks }),
}) {}

const RecordBatchFromSelf = Schema.declare<RecordBatch>((value) => value instanceof RecordBatch, {
  title: "RecordBatchFromSelf",
  description: "a record batch",
})

const parseRecordBatch: <A, R>(
  from: Schema.Schema<Uint8Array, A, R>,
) => Schema.Schema<RecordBatch, A, R> = Schema.transformOrFail(RecordBatchFromSelf, {
  decode: (value, _, ast) =>
    ParseResult.try({
      catch: () => new ParseResult.Type(ast, value, "Failed to read record batch"),
      try: () => {
        const reader = RecordBatchReader.from(value)
        const [batch] = reader.readAll()
        return batch
      },
    }),
  encode: (value, _, ast) =>
    ParseResult.try({
      catch: () => new ParseResult.Type(ast, value, "Failed to write record batch"),
      try: () => {
        const writer = new RecordBatchWriter()
        writer.write(value)
        writer.finish()
        return writer.toUint8Array(true)
      },
    }),
})

const RecordBatchFromUint8Array = parseRecordBatch(Schema.Uint8ArrayFromSelf)

/**
 * A record batch with metadata.
 */
export class ResponseBatch extends Schema.TaggedClass<ResponseBatch>()("ResponseBatch", {
  data: RecordBatchFromUint8Array,
  metadata: RecordBatchMetadata,
}) {}
