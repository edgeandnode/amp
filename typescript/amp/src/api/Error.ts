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
 * DatasetDefStoreError - Failure in dataset definition store operations.
 *
 * Causes:
 * - Failed to read dataset definition files
 * - Invalid dataset definition format
 * - File system access errors
 * - Configuration directory issues
 *
 * Applies to:
 * - POST /datasets - When loading dataset definitions for registration
 * - Any operation that requires reading dataset definition files
 */
export class DatasetDefStoreError extends Schema.Class<DatasetDefStoreError>("DatasetDefStoreError")(
  {
    code: Schema.Literal("DATASET_DEF_STORE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "DatasetDefStoreError" as const
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
 * - GET /locations - Invalid pagination parameters
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
 * - GET /locations - When limit exceeds maximum
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
 * - GET /locations - When limit is 0 or negative
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
 * DatasetAlreadyExists - The dataset already exists with the provided manifest.
 *
 * Causes:
 * - Attempting to register a dataset that already exists when a manifest is provided
 * - Conflict between existing dataset and new manifest
 *
 * Applies to:
 * - POST /datasets - When dataset exists and manifest is provided
 * - POST /datasets - When dataset exists and manifest is provided
 */
export class DatasetAlreadyExists extends Schema.Class<DatasetAlreadyExists>("DatasetAlreadyExists")(
  {
    code: Schema.Literal("DATASET_ALREADY_EXISTS").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 409,
  },
) {
  readonly _tag = "DatasetAlreadyExists" as const
}

/**
 * ManifestRequired - Dataset not found and manifest is required but not provided.
 *
 * Causes:
 * - Dataset does not exist in registry
 * - No manifest provided with registration request
 * - Cannot register without dataset definition
 *
 * Applies to:
 * - POST /datasets - When dataset not found and no manifest provided
 * - POST /datasets - When dataset not found and no manifest provided
 */
export class ManifestRequired extends Schema.Class<ManifestRequired>("ManifestRequired")(
  {
    code: Schema.Literal("MANIFEST_REQUIRED").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "ManifestRequired" as const
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
 * - Invalid SQL syntax in query
 * - Referenced tables or datasets don't exist
 * - Type mismatches in query
 * - Unsupported SQL features
 * - Schema inference failures
 *
 * Applies to:
 * - Query execution operations
 * - Schema inference operations
 */
export class PlanningError extends Schema.Class<PlanningError>("PlanningError")(
  {
    code: Schema.Literal("PLANNING_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
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
