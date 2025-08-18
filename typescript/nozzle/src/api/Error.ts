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
 * - POST /datasets/{id}/dump - When scheduling dataset dumps
 * - POST /deploy - When scheduling deployment jobs
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
 * - POST /deploy - When loading dataset definitions for deployment
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
 * - Dataset not yet deployed
 *
 * Applies to:
 * - GET /datasets/{id} - When dataset ID doesn't exist
 * - POST /datasets/{id}/dump - When attempting to dump non-existent dataset
 * - Query operations referencing non-existent datasets
 *
 * Note: HTTP status is 400 (not 404) to match Rust implementation
 */
export class DatasetNotFound extends Schema.Class<DatasetNotFound>("DatasetNotFound")(
  {
    code: Schema.Literal("DATASET_NOT_FOUND").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "DatasetNotFound" as const
}

/**
 * InvalidDatasetId - The provided dataset ID is malformed or invalid.
 *
 * Causes:
 * - Dataset ID contains invalid characters
 * - Dataset ID format does not match expected pattern
 * - Empty or null dataset ID
 *
 * Applies to:
 * - GET /datasets/{id} - When ID format is invalid
 * - POST /datasets/{id}/dump - When ID format is invalid
 * - Any endpoint accepting dataset ID parameter
 */
export class InvalidDatasetId extends Schema.Class<InvalidDatasetId>("InvalidDatasetId")(
  {
    code: Schema.Literal("INVALID_DATASET_ID").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 400,
  },
) {
  readonly _tag = "InvalidDatasetId" as const
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
 * - POST /deploy - Invalid deployment request
 * - POST /datasets/{id}/dump - Invalid dump parameters
 * - Any endpoint with request validation
 *
 * Note: HTTP status is 404 (not 400) to match Rust implementation
 */
export class InvalidRequest extends Schema.Class<InvalidRequest>("InvalidRequest")(
  {
    code: Schema.Literal("INVALID_REQUEST").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 404,
  },
) {
  readonly _tag = "InvalidRequest" as const
}

/**
 * ManifestParseError - Failed to parse dataset manifest file.
 *
 * Causes:
 * - Invalid TOML syntax in manifest
 * - Invalid JSON syntax in manifest
 * - Missing required manifest fields
 * - Type mismatches in manifest values
 * - Unsupported manifest version
 *
 * Applies to:
 * - Dataset loading operations
 * - Manifest validation during deployment
 * - Generated from DatasetStoreError::ManifestError
 *
 * Related to: InvalidManifest (for semantic validation errors)
 */
export class ManifestParseError extends Schema.Class<ManifestParseError>("ManifestParseError")(
  {
    code: Schema.Literal("MANIFEST_PARSE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "ManifestParseError" as const
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
 * - POST /deploy - During manifest validation
 * - Dataset initialization
 * - Different from ManifestParseError (syntax vs semantics)
 */
export class InvalidManifest extends Schema.Class<InvalidManifest>("InvalidManifest")(
  {
    code: Schema.Literal("INVALID_MANIFEST").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
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
 * SqlParseError - Failed to parse SQL query or dataset definition.
 *
 * Causes:
 * - Invalid SQL syntax in dataset definitions
 * - Malformed SQL queries in requests
 * - Unsupported SQL features or functions
 * - Invalid table/column references
 * - SQL injection attempts blocked by parser
 *
 * Applies to:
 * - SQL dataset definitions during deployment
 * - Query operations with invalid SQL
 * - Dataset validation operations
 * - Generated from DatasetStoreError::SqlParseError
 */
export class SqlParseError extends Schema.Class<SqlParseError>("SqlParseError")(
  {
    code: Schema.Literal("SQL_PARSE_ERROR").pipe(Schema.propertySignature, Schema.fromKey("error_code")),
    message: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("error_message")),
  },
  {
    [HttpApiSchema.AnnotationStatus]: 500,
  },
) {
  readonly _tag = "SqlParseError" as const
}

/**
 * PlanningError - Failed to create query execution plan.
 *
 * Causes:
 * - Invalid table references in query
 * - Unsupported query operations
 * - Query optimizer failures
 * - Resource constraints exceeded
 * - Invalid join conditions
 * - Schema mismatch between datasets
 *
 * Applies to:
 * - Query execution via Arrow Flight or JSON Lines API
 * - SQL dataset validation during deployment
 * - Complex queries with multiple datasets
 * - Generated from query planning phase failures
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
