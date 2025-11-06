import * as Effect from "effect/Effect"
import type * as ParseResult from "effect/ParseResult"
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
    description:
      "the name of the dataset (must start with lowercase letter or underscore, followed by lowercase letters, digits, or underscores)",
    examples: ["uniswap", "eth_mainnet", "_test"],
  }),
)
export type DatasetName = Schema.Schema.Type<typeof DatasetName>

export const Network = Schema.Lowercase.pipe(
  Schema.annotations({
    title: "Network",
    description: "the network of a dataset or provider",
    examples: ["mainnet"],
  }),
)

export const DatasetKind = Schema.Literal("manifest", "evm-rpc", "eth-beacon", "firehose").pipe(
  Schema.annotations({
    title: "Kind",
    description: "the kind of dataset",
    examples: ["manifest", "evm-rpc", "eth-beacon", "firehose"],
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

export const DatasetHash = Schema.String.pipe(
  Schema.pattern(/^[0-9a-fA-F]{64}$/),
  Schema.length(64),
  Schema.annotations({
    title: "Hash",
    description: "a 32-byte SHA-256 hash (64 hex characters)",
    examples: ["b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"],
  }),
)
export type DatasetHash = Schema.Schema.Type<typeof DatasetHash>

export const DatasetLatestTag = Schema.Literal("latest").pipe(
  Schema.annotations({
    title: "LatestTag",
    description: "the 'latest' tag pointing to the most recent version",
  }),
)
export type DatasetLatestTag = Schema.Schema.Type<typeof DatasetLatestTag>

export const DatasetDevTag = Schema.Literal("dev").pipe(
  Schema.annotations({
    title: "DevTag",
    description: "the 'dev' tag for development versions",
  }),
)
export type DatasetDevTag = Schema.Schema.Type<typeof DatasetDevTag>

export const DatasetRevision = Schema.Union(
  DatasetVersion,
  DatasetHash,
  DatasetLatestTag,
  DatasetDevTag,
).pipe(
  Schema.annotations({
    title: "Revision",
    description: "a dataset revision reference (semver tag, 64-char hex hash, 'latest', or 'dev')",
    examples: ["1.0.0", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", "latest", "dev"],
  }),
)
export type DatasetRevision = Schema.Schema.Type<typeof DatasetRevision>

export const DatasetReferenceStr = Schema.String.pipe(
  Schema.pattern(/^[a-z0-9_]+\/[a-z_][a-z0-9_]*@.+$/),
  Schema.annotations({
    title: "DatasetReferenceStr",
    description: "a dataset reference string in the format namespace/name@revision (version, hash, 'latest', or 'dev')",
    examples: [
      "edgeandnode/mainnet@1.0.0",
      "edgeandnode/mainnet@latest",
      "edgeandnode/mainnet@dev",
      "0xdeadbeef/eth_firehose@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
    ],
  }),
)
export type DatasetReferenceStr = Schema.Schema.Type<typeof DatasetReferenceStr>

export class DatasetReference extends Schema.Class<DatasetReference>("DatasetReference")({
  namespace: DatasetNamespace,
  name: DatasetName,
  revision: DatasetRevision,
}) {}

/**
 * Parses a dataset reference string to extract and validate namespace, name, and revision components.
 *
 * @param reference - The reference string to parse in format "namespace/name@revision"
 * @returns An Effect that yields a DatasetReference instance
 *
 * @example
 * const ref = yield* parseDatasetReference("edgeandnode/mainnet@1.0.0")
 * // DatasetReference { namespace: "edgeandnode", name: "mainnet", revision: "1.0.0" }
 *
 * const ref = yield* parseDatasetReference("edgeandnode/mainnet@latest")
 * // DatasetReference { namespace: "edgeandnode", name: "mainnet", revision: "latest" }
 */
export const parseDatasetReference = (
  reference: DatasetReferenceStr,
): Effect.Effect<DatasetReference, ParseResult.ParseError> => {
  return Effect.gen(function*() {
    const atIndex = reference.lastIndexOf("@")
    const slashIndex = reference.indexOf("/")

    // Extract and validate each component
    const namespace = yield* Schema.decodeUnknown(DatasetNamespace)(reference.substring(0, slashIndex))
    const name = yield* Schema.decodeUnknown(DatasetName)(reference.substring(slashIndex + 1, atIndex))
    const revision = yield* Schema.decodeUnknown(DatasetRevision)(reference.substring(atIndex + 1))

    return new DatasetReference({ namespace, name, revision })
  })
}

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

export const DatasetLicense = Schema.String.pipe(
  Schema.annotations({
    title: "License",
    description: "License covering the dataset",
    examples: ["MIT"],
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
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  description: DatasetDescription.pipe(Schema.optional),
  keywords: Schema.Array(DatasetKeyword).pipe(Schema.optional),
  license: DatasetLicense.pipe(Schema.optional),
}) {}

export class DatasetConfig extends Schema.Class<DatasetConfig>(
  "DatasetConfig",
)({
  namespace: DatasetNamespace.pipe(Schema.optional),
  name: DatasetName,
  network: Network,
  readme: DatasetReadme.pipe(Schema.optional),
  repository: DatasetRepository.pipe(Schema.optional),
  description: DatasetDescription.pipe(Schema.optional),
  keywords: Schema.Array(DatasetKeyword).pipe(Schema.optional),
  dependencies: Schema.Record({ key: Schema.String, value: DatasetReferenceStr }),
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
  dependencies: Schema.Record({ key: Schema.String, value: DatasetReferenceStr }),
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
 *
 * Future kinds: DatasetFirehose, DatasetEthBeacon, etc.
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
