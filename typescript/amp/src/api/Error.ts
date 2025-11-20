import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as Schema from "effect/Schema"

/**
 * SchedulerError - Indicates a failure in the job scheduling system.
 *
 * Causes:
 * - Failed to schedule a dump job
 * - Worker pool unavailable
 * - Internal scheduler state errors
 *
 * Applies to:
 * - POST /datasets/{name}/dump - When scheduling dataset dumps
 * - POST /datasets - When scheduling registration jobs
 */
export class SchedulerError extends Schema.Class<SchedulerError>("SchedulerError")(
  {
    code: Schema.Literal("SCHEDULER_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "SchedulerError" as const
}

/**
 * MetadataDbError - Database operation failure in the metadata PostgreSQL database.
 *
 * Causes:
 * - Database connection failures
 * - SQL execution errors
 * - Database migration issues
 * - Worker notification send/receive failures
 * - Data consistency errors (e.g., multiple active locations)
 *
 * Applies to:
 * - Any operation that queries or updates metadata
 * - Worker coordination operations
 * - Dataset state tracking
 */
export class MetadataDbError extends Schema.Class<MetadataDbError>("MetadataDbError")(
  {
    code: Schema.Literal("METADATA_DB_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "MetadataDbError" as const
}

/**
 * DatasetStoreError - Failure in dataset storage operations.
 *
 * Causes:
 * - File/object store retrieval failures
 * - Manifest parsing errors (TOML/JSON)
 * - Unsupported dataset kind
 * - Dataset name validation failures
 * - Schema validation errors (missing or mismatched)
 * - Provider configuration not found
 * - SQL parsing failures in dataset definitions
 *
 * Applies to:
 * - GET /datasets - Listing datasets
 * - GET /datasets/{id} - Retrieving specific dataset
 * - POST /datasets/{id}/dump - When loading dataset definitions
 * - Query operations that access dataset metadata
 */
export class DatasetStoreError extends Schema.Class<DatasetStoreError>("DatasetStoreError")(
  {
    code: Schema.Literal("DATASET_STORE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "DatasetStoreError" as const
}

/**
 * DatasetNotFound - The requested dataset does not exist.
 *
 * Causes:
 * - Dataset ID does not exist in the system
 * - Dataset has been deleted
 * - Dataset not yet registered
 *
 * Applies to:
 * - GET /datasets/{id} - When dataset ID doesn't exist
 * - POST /datasets/{id}/dump - When attempting to dump non-existent dataset
 * - Query operations referencing non-existent datasets
 */
export class DatasetNotFound extends Schema.Class<DatasetNotFound>("DatasetNotFound")(
  {
    code: Schema.Literal("DATASET_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "DatasetNotFound" as const
}

/**
 * TableNotFoundInDataset - Table not found in dataset.
 *
 * Causes:
 * - Table name referenced in SQL query does not exist in the dataset
 * - Table name is misspelled
 * - Dataset does not contain the referenced table
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries with invalid table references
 */
export class TableNotFoundInDataset extends Schema.Class<TableNotFoundInDataset>("TableNotFoundInDataset")(
  {
    code: Schema.Literal("TABLE_NOT_FOUND_IN_DATASET").pipe(
      Schema.propertySignature,
      Schema.fromKey("error_code"),
    ),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "TableNotFoundInDataset" as const
}

/**
 * InvalidSelector - The provided dataset selector (name/version) is malformed or invalid.
 *
 * Causes:
 * - Dataset name contains invalid characters or doesn't follow naming conventions
 * - Dataset name is empty or malformed
 * - Version syntax is invalid (e.g., malformed semver)
 * - Path parameter extraction fails for dataset selection
 *
 * Applies to:
 * - GET /datasets/{name} - When dataset name format is invalid
 * - GET /datasets/{name}/versions/{version} - When name or version format is invalid
 * - Any endpoint accepting dataset selector parameters
 */
export class InvalidSelector extends Schema.Class<InvalidSelector>("InvalidSelector")(
  {
    code: Schema.Literal("INVALID_SELECTOR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidSelector" as const
}

/**
 * InvalidRequest - The request is malformed or contains invalid parameters.
 *
 * Causes:
 * - Missing required request parameters
 * - Invalid parameter values
 * - Malformed request body
 * - Invalid content type
 * - Request validation failures
 *
 * Applies to:
 * - POST /datasets - Invalid registration request
 * - POST /datasets/{id}/dump - Invalid dump parameters
 * - Any endpoint with request validation
 */
export class InvalidRequest extends Schema.Class<InvalidRequest>("InvalidRequest")(
  {
    code: Schema.Literal("INVALID_REQUEST").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidRequest" as const
}

/**
 * InvalidManifest - Dataset manifest is semantically invalid.
 *
 * Causes:
 * - Invalid dataset references in SQL views
 * - Circular dependencies between datasets
 * - Invalid provider references
 * - Schema validation failures
 * - Invalid dataset configuration
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 * - Dataset initialization
 * - Different from ManifestParseError (syntax vs semantics)
 */
export class InvalidManifest extends Schema.Class<InvalidManifest>("InvalidManifest")(
  {
    code: Schema.Literal("INVALID_MANIFEST").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidManifest" as const
}

/**
 * UnexpectedJobStatus - Job is in an unexpected state for the requested operation.
 *
 * Causes:
 * - Attempting to schedule a dump when one is already running
 * - Job state machine violations
 * - Concurrent job modification attempts
 * - Invalid state transitions
 *
 * Applies to:
 * - POST /datasets/{id}/dump - When a dump is already in progress
 * - Job management operations
 *
 * Example: Trying to start a dump job when status is already "Running"
 */
export class UnexpectedJobStatus extends Schema.Class<UnexpectedJobStatus>("UnexpectedJobStatus")(
  {
    code: Schema.Literal("UNEXPECTED_JOB_STATUS").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "UnexpectedJobStatus" as const
}

/**
 * JobNotFound - The requested job does not exist.
 *
 * Causes:
 * - Job ID does not exist in the system
 * - Job has been deleted
 * - Job has completed and been cleaned up
 *
 * Applies to:
 * - GET /jobs/{id} - When job ID doesn't exist
 * - DELETE /jobs/{id} - When attempting to delete non-existent job
 * - PUT /jobs/{id}/stop - When attempting to stop non-existent job
 */
export class JobNotFound extends Schema.Class<JobNotFound>("JobNotFound")(
  {
    code: Schema.Literal("JOB_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "JobNotFound" as const
}

/**
 * InvalidQueryParameters - The query parameters are invalid or malformed.
 *
 * Causes:
 * - Invalid integer format for limit or pagination cursors
 * - Malformed query string syntax
 * - Missing required query parameters
 * - Query parameter validation failures
 *
 * Applies to:
 * - GET /jobs - Invalid pagination parameters
 * - Any endpoint with query parameter validation
 */
export class InvalidQueryParameters extends Schema.Class<InvalidQueryParameters>("InvalidQueryParameters")(
  {
    code: Schema.Literal("INVALID_QUERY_PARAMETERS").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidQueryParameters" as const
}

/**
 * LimitTooLarge - The requested limit exceeds the maximum allowed value.
 *
 * Causes:
 * - Limit parameter greater than maximum page size (1000)
 * - Attempting to request too many records at once
 * - Pagination limit validation failure
 *
 * Applies to:
 * - GET /jobs - When limit exceeds maximum
 * - Any paginated endpoint with limit validation
 */
export class LimitTooLarge extends Schema.Class<LimitTooLarge>("LimitTooLarge")(
  {
    code: Schema.Literal("LIMIT_TOO_LARGE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "LimitTooLarge" as const
}

/**
 * LimitInvalid - The requested limit is invalid (zero or negative).
 *
 * Causes:
 * - Limit parameter is 0 or negative
 * - Invalid limit format or type
 * - Limit validation failure
 *
 * Applies to:
 * - GET /jobs - When limit is 0 or negative
 * - Any paginated endpoint with limit validation
 */
export class LimitInvalid extends Schema.Class<LimitInvalid>("LimitInvalid")(
  {
    code: Schema.Literal("LIMIT_INVALID").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "LimitInvalid" as const
}

/**
 * ManifestRegistrationError - Failed to register manifest in the system.
 *
 * Causes:
 * - Internal error during manifest registration
 * - Registry service unavailable
 * - Manifest storage failure
 *
 * Applies to:
 * - POST /datasets - During manifest registration
 * - POST /datasets - During manifest registration
 */
export class ManifestRegistrationError extends Schema.Class<ManifestRegistrationError>("ManifestRegistrationError")(
  {
    code: Schema.Literal("MANIFEST_REGISTRATION_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "ManifestRegistrationError" as const
}

/**
 * ManifestLinkingError - Failed to link manifest to dataset.
 *
 * Causes:
 * - Error during manifest linking in metadata database
 * - Error updating dev tag
 * - Database transaction failure
 * - Foreign key constraint violations
 *
 * Applies to:
 * - POST /datasets - During manifest linking to dataset
 */
export class ManifestLinkingError extends Schema.Class<ManifestLinkingError>("ManifestLinkingError")(
  {
    code: Schema.Literal("MANIFEST_LINKING_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "ManifestLinkingError" as const
}

/**
 * VersionTaggingError - Failed to tag version for the dataset.
 *
 * Causes:
 * - Error during version tagging in metadata database
 * - Invalid semantic version format
 * - Error updating latest tag
 * - Database constraint violations
 *
 * Applies to:
 * - POST /datasets - During version tagging
 */
export class VersionTaggingError extends Schema.Class<VersionTaggingError>("VersionTaggingError")(
  {
    code: Schema.Literal("VERSION_TAGGING_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "VersionTaggingError" as const
}

/**
 * ManifestValidationError - Manifest validation error.
 *
 * Causes:
 * - SQL queries contain non-incremental operations
 * - Invalid table references in SQL
 * - Schema validation failures
 * - Type inference errors
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 */
export class ManifestValidationError extends Schema.Class<ManifestValidationError>("ManifestValidationError")(
  {
    code: Schema.Literal("MANIFEST_VALIDATION_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "ManifestValidationError" as const
}

/**
 * UnsupportedDatasetKind - Dataset kind is not supported.
 *
 * Causes:
 * - Dataset kind is not one of the supported types (manifest, evm-rpc, firehose, eth-beacon)
 * - Invalid or unknown dataset kind value
 * - Legacy dataset kinds that are no longer supported
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 */
export class UnsupportedDatasetKind extends Schema.Class<UnsupportedDatasetKind>("UnsupportedDatasetKind")(
  {
    code: Schema.Literal("UNSUPPORTED_DATASET_KIND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "UnsupportedDatasetKind" as const
}

/**
 * ManifestNotFound - Manifest with the provided hash not found.
 *
 * Causes:
 * - A manifest hash was provided but the manifest doesn't exist in the system
 * - The hash is valid format but no manifest is stored with that hash
 * - Manifest was deleted or never registered
 *
 * Applies to:
 * - POST /datasets - When linking to a manifest hash that doesn't exist
 */
export class ManifestNotFound extends Schema.Class<ManifestNotFound>("ManifestNotFound")(
  {
    code: Schema.Literal("MANIFEST_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "ManifestNotFound" as const
}

/**
 * InvalidPayloadFormat - Invalid request payload format.
 *
 * Causes:
 * - Request JSON is malformed or invalid
 * - Required fields are missing or have wrong types
 * - Dataset name or version format is invalid
 * - JSON deserialization failures
 *
 * Applies to:
 * - POST /datasets - When request body is invalid
 * - POST /schema - When request payload cannot be parsed
 */
export class InvalidPayloadFormat extends Schema.Class<InvalidPayloadFormat>("InvalidPayloadFormat")(
  {
    code: Schema.Literal("INVALID_PAYLOAD_FORMAT").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidPayloadFormat" as const
}

/**
 * StoreError - Dataset store operation error.
 *
 * Causes:
 * - Failed to load dataset from store
 * - Dataset store configuration errors
 * - Dataset store connectivity issues
 * - Object store access failures
 *
 * Applies to:
 * - POST /datasets - During dataset store operations
 */
export class StoreError extends Schema.Class<StoreError>("StoreError")(
  {
    code: Schema.Literal("STORE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "StoreError" as const
}

/**
 * InvalidJobId - The provided job ID is malformed or invalid.
 *
 * Causes:
 * - Job ID contains invalid characters
 * - Job ID format does not match expected pattern
 * - Empty or null job ID
 * - Job ID is not a valid integer
 *
 * Applies to:
 * - GET /jobs/{id} - When ID format is invalid
 * - DELETE /jobs/{id} - When ID format is invalid
 * - PUT /jobs/{id}/stop - When ID format is invalid
 */
export class InvalidJobId extends Schema.Class<InvalidJobId>("InvalidJobId")(
  {
    code: Schema.Literal("INVALID_JOB_ID").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidJobId" as const
}

/**
 * RegistrationFailed - Failed to register manifest in the registry system.
 *
 * Causes:
 * - Dataset already exists with the given name and version
 * - Dataset store error during manifest storage
 * - Database error during registry operations
 * - Serialization error when processing manifest
 *
 * Applies to:
 * - POST /register - When manifest registration fails
 */
export class RegistrationFailed extends Schema.Class<RegistrationFailed>("RegistrationFailed")(
  {
    code: Schema.Literal("REGISTRATION_FAILED").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "RegistrationFailed" as const
}

/**
 * PlanningError - Query planning failure.
 *
 * Causes:
 * - Query planning fails due to invalid references
 * - Type inference failures
 * - Schema determination errors
 * - Internal DataFusion planning errors
 *
 * Applies to:
 * - POST /schema - When determining output schema
 * - Query execution operations
 */
export class PlanningError extends Schema.Class<PlanningError>("PlanningError")(
  {
    code: Schema.Literal("PLANNING_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "PlanningError" as const
}

/**
 * CatalogForSqlError - Failure to build catalog for SQL query.
 *
 * Causes:
 * - Failed to load datasets referenced in SQL query
 * - Dataset store unavailable for catalog construction
 * - Invalid dataset references in query
 * - Catalog initialization failures
 *
 * Applies to:
 * - Query execution operations
 * - Schema inference operations (POST /schema)
 */
export class CatalogForSqlError extends Schema.Class<CatalogForSqlError>("CatalogForSqlError")(
  {
    code: Schema.Literal("CATALOG_FOR_SQL_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "CatalogForSqlError" as const
}

/**
 * UnqualifiedTable - Table reference is not qualified with a dataset.
 *
 * Causes:
 * - SQL query contains a table reference without a schema/dataset qualifier
 * - All tables must be qualified with a dataset reference (e.g., dataset.table)
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with unqualified table references
 */
export class UnqualifiedTable extends Schema.Class<UnqualifiedTable>("UnqualifiedTable")(
  {
    code: Schema.Literal("UNQUALIFIED_TABLE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "UnqualifiedTable" as const
}

/**
 * CatalogQualifiedTable - Table reference includes a catalog qualifier.
 *
 * Causes:
 * - SQL query contains a catalog-qualified table reference (catalog.schema.table)
 * - Only dataset-qualified tables are supported (dataset.table)
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with catalog-qualified table references
 */
export class CatalogQualifiedTable extends Schema.Class<CatalogQualifiedTable>("CatalogQualifiedTable")(
  {
    code: Schema.Literal("CATALOG_QUALIFIED_TABLE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "CatalogQualifiedTable" as const
}

/**
 * InvalidTableName - Table name does not conform to SQL identifier rules.
 *
 * Causes:
 * - Table name contains invalid characters
 * - Table name doesn't follow naming conventions
 * - Table name exceeds maximum length
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with invalid table names
 */
export class InvalidTableName extends Schema.Class<InvalidTableName>("InvalidTableName")(
  {
    code: Schema.Literal("INVALID_TABLE_NAME").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidTableName" as const
}

/**
 * InvalidSchemaReference - Schema portion of table reference is not a valid dataset reference.
 *
 * Causes:
 * - Schema/dataset reference format is invalid
 * - Dataset reference doesn't conform to expected format (namespace/name@version)
 * - Invalid characters or structure in dataset reference
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with invalid schema references
 */
export class InvalidSchemaReference extends Schema.Class<InvalidSchemaReference>("InvalidSchemaReference")(
  {
    code: Schema.Literal("INVALID_SCHEMA_REFERENCE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidSchemaReference" as const
}

/**
 * ResolveHashError - Failed to resolve dataset reference to hash.
 *
 * Causes:
 * - Dataset store cannot resolve a reference to content hash
 * - Storage backend errors
 * - Invalid reference format
 *
 * Applies to:
 * - POST /schema - When resolving dataset references
 */
export class ResolveHashError extends Schema.Class<ResolveHashError>("ResolveHashError")(
  {
    code: Schema.Literal("RESOLVE_HASH_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "ResolveHashError" as const
}

/**
 * GetDatasetError - Failed to retrieve dataset from store.
 *
 * Causes:
 * - Dataset manifest is invalid or corrupted
 * - Unsupported dataset kind
 * - Storage backend errors when reading dataset
 *
 * Applies to:
 * - POST /schema - When loading dataset definitions
 */
export class GetDatasetError extends Schema.Class<GetDatasetError>("GetDatasetError")(
  {
    code: Schema.Literal("GET_DATASET_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "GetDatasetError" as const
}

/**
 * EthCallUdfCreationError - Failed to create ETH call UDF.
 *
 * Causes:
 * - Invalid provider configuration for dataset
 * - Provider connection issues
 * - Dataset is not an EVM RPC dataset but eth_call was requested
 *
 * Applies to:
 * - POST /schema - When creating ETH call UDFs
 */
export class EthCallUdfCreationError extends Schema.Class<EthCallUdfCreationError>("EthCallUdfCreationError")(
  {
    code: Schema.Literal("ETH_CALL_UDF_CREATION_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "EthCallUdfCreationError" as const
}

/**
 * InvalidFunctionReference - Invalid function reference in SQL.
 *
 * Causes:
 * - Function's dataset qualifier cannot be parsed as valid dataset reference
 * - Invalid function qualification format
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries with dataset-qualified functions
 */
export class InvalidFunctionReference extends Schema.Class<InvalidFunctionReference>("InvalidFunctionReference")(
  {
    code: Schema.Literal("INVALID_FUNCTION_REFERENCE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidFunctionReference" as const
}

/**
 * InvalidFunctionFormat - Invalid function format in SQL.
 *
 * Causes:
 * - Function names must be either unqualified or dataset-qualified
 * - Function has more than two parts
 * - Invalid function naming format
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries with functions
 */
export class InvalidFunctionFormat extends Schema.Class<InvalidFunctionFormat>("InvalidFunctionFormat")(
  {
    code: Schema.Literal("INVALID_FUNCTION_FORMAT").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidFunctionFormat" as const
}

/**
 * SqlParseError - SQL parse error.
 *
 * Causes:
 * - SQL query has invalid syntax
 * - Unsupported SQL features are used
 * - Query parsing fails
 *
 * Applies to:
 * - POST /schema - When parsing SQL queries
 */
export class SqlParseError extends Schema.Class<SqlParseError>("SqlParseError")(
  {
    code: Schema.Literal("SQL_PARSE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "SqlParseError" as const
}

/**
 * FunctionNotFoundInDataset - Function not found in referenced dataset.
 *
 * Causes:
 * - SQL query references a function that doesn't exist in the dataset
 * - Function name is misspelled or dataset doesn't define the function
 *
 * Applies to:
 * - POST /schema - When resolving function references
 */
export class FunctionNotFoundInDataset extends Schema.Class<FunctionNotFoundInDataset>("FunctionNotFoundInDataset")(
  {
    code: Schema.Literal("FUNCTION_NOT_FOUND_IN_DATASET").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "FunctionNotFoundInDataset" as const
}

/**
 * EthCallNotAvailable - eth_call function not available for dataset.
 *
 * Causes:
 * - eth_call function is referenced in SQL but dataset doesn't support it
 * - Dataset is not an EVM RPC dataset
 *
 * Applies to:
 * - POST /schema - When checking eth_call availability
 */
export class EthCallNotAvailable extends Schema.Class<EthCallNotAvailable>("EthCallNotAvailable")(
  {
    code: Schema.Literal("ETH_CALL_NOT_AVAILABLE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "EthCallNotAvailable" as const
}

/**
 * DependencyNotFound - Dependency not found in dataset store.
 *
 * Causes:
 * - Referenced dependency does not exist in dataset store
 * - Specified version or hash cannot be found
 *
 * Applies to:
 * - POST /schema - When resolving dependencies
 */
export class DependencyNotFound extends Schema.Class<DependencyNotFound>("DependencyNotFound")(
  {
    code: Schema.Literal("DEPENDENCY_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "DependencyNotFound" as const
}

/**
 * DependencyAliasNotFound - Dependency alias not found in dependencies map.
 *
 * Causes:
 * - Table reference uses an alias not provided in dependencies
 * - Function reference uses an alias not provided in dependencies
 *
 * Applies to:
 * - POST /schema - When looking up dependency aliases
 */
export class DependencyAliasNotFound extends Schema.Class<DependencyAliasNotFound>("DependencyAliasNotFound")(
  {
    code: Schema.Literal("DEPENDENCY_ALIAS_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "DependencyAliasNotFound" as const
}

/**
 * EmptyTablesAndFunctions - No tables or functions provided.
 *
 * Causes:
 * - At least one table or function is required for schema analysis
 *
 * Applies to:
 * - POST /schema - When both tables and functions fields are empty
 */
export class EmptyTablesAndFunctions extends Schema.Class<EmptyTablesAndFunctions>("EmptyTablesAndFunctions")(
  {
    code: Schema.Literal("EMPTY_TABLES_AND_FUNCTIONS").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "EmptyTablesAndFunctions" as const
}

/**
 * InvalidTableSql - SQL syntax error in table definition.
 *
 * Causes:
 * - Query parsing fails
 *
 * Applies to:
 * - POST /schema - When analyzing table SQL queries
 */
export class InvalidTableSql extends Schema.Class<InvalidTableSql>("InvalidTableSql")(
  {
    code: Schema.Literal("INVALID_TABLE_SQL").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidTableSql" as const
}

/**
 * TableReferenceResolution - Failed to extract table references from SQL.
 *
 * Causes:
 * - Invalid table reference format encountered
 *
 * Applies to:
 * - POST /schema - When resolving table references
 */
export class TableReferenceResolution extends Schema.Class<TableReferenceResolution>("TableReferenceResolution")(
  {
    code: Schema.Literal("TABLE_REFERENCE_RESOLUTION").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "TableReferenceResolution" as const
}

/**
 * FunctionReferenceResolution - Failed to resolve function references from SQL.
 *
 * Causes:
 * - Unsupported DML statements encountered
 *
 * Applies to:
 * - POST /schema - When resolving function references
 */
export class FunctionReferenceResolution
  extends Schema.Class<FunctionReferenceResolution>("FunctionReferenceResolution")(
    {
      code: Schema.Literal("FUNCTION_REFERENCE_RESOLUTION").pipe(
        Schema.propertySignature,
        Schema.fromKey("error_code"),
      ),
      message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
    },
    {
      [HttpApiSchema.AnnotationStatus]: 500,
    },
  )
{
  readonly _tag = "FunctionReferenceResolution" as const
}

/**
 * DependencyResolution - Failed to resolve dependency.
 *
 * Causes:
 * - Database query fails during resolution
 *
 * Applies to:
 * - POST /schema - When resolving dependencies
 */
export class DependencyResolution extends Schema.Class<DependencyResolution>("DependencyResolution")(
  {
    code: Schema.Literal("DEPENDENCY_RESOLUTION").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "DependencyResolution" as const
}

/**
 * SchemaInference - Failed to infer output schema from query.
 *
 * Causes:
 * - Schema determination encounters errors
 *
 * Applies to:
 * - POST /schema - When inferring output schema
 */
export class SchemaInference extends Schema.Class<SchemaInference>("SchemaInference")(
  {
    code: Schema.Literal("SCHEMA_INFERENCE").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "SchemaInference" as const
}
