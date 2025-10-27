import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiClient from "@effect/platform/HttpApiClient"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Model from "../Model.ts"
import * as Error from "./Error.ts"

/**
 * The dataset namespace parameter.
 */
const datasetNamespace = HttpApiSchema.param("namespace", Schema.String)

/**
 * The dataset name parameter.
 */
const datasetName = HttpApiSchema.param("name", Model.DatasetName)

/**
 * The dataset revision parameter (version, hash, "latest", or "dev").
 */
const datasetRevision = HttpApiSchema.param("revision", Schema.String)

/**
 * The job ID parameter (GET /jobs/{jobId}, DELETE /jobs/{jobId}, PUT /jobs/{jobId}/stop).
 */
const jobId = HttpApiSchema.param("jobId", Model.JobIdParam)

/**
 * The location ID parameter (GET /locations/{locationId}, DELETE /locations/{locationId}).
 */
const locationId = HttpApiSchema.param("locationId", Model.LocationIdParam)

/**
 * The register dataset endpoint (POST /datasets).
 */
const registerDataset = HttpApiEndpoint.post("registerDataset")`/datasets`
  .addError(Error.InvalidRequest)
  .addError(Error.InvalidManifest)
  .addError(Error.ManifestRegistrationError)
  .addError(Error.DatasetAlreadyExists)
  .addError(Error.DatasetDefStoreError)
  .addError(Error.DatasetStoreError)
  .addSuccess(Schema.Void, { status: 201 })
  .setPayload(
    Schema.Struct({
      namespace: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("namespace")),
      name: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("name")),
      version: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("version")),
      manifest: Schema.parseJson(Schema.Union(Model.DatasetManifest, Model.DatasetRpc)),
    }),
  )

/**
 * Error type for the `registerDataset` endpoint.
 *
 * - InvalidRequest: Invalid request parameters or dataset name format.
 * - InvalidManifest: The manifest is semantically invalid.
 * - ManifestRegistrationError: Failed to register manifest in system.
 * - DatasetAlreadyExists: Dataset exists and manifest provided (conflict).
 * - DatasetDefStoreError: Failure in dataset definition store operations.
 * - DatasetStoreError: Failed to load dataset from store.
 */
export type RegisterDatasetError =
  | Error.InvalidRequest
  | Error.InvalidManifest
  | Error.ManifestRegistrationError
  | Error.DatasetAlreadyExists
  | Error.DatasetDefStoreError
  | Error.DatasetStoreError

/**
 * The get datasets endpoint (GET /datasets).
 */
const getDatasets = HttpApiEndpoint.get("getDatasets")`/datasets`
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.DatasetsResponse)

/**
 * Error type for the `getDatasets` endpoint.
 *
 * - DatasetStoreError: Failed to retrieve datasets from the dataset store.
 * - MetadataDbError: Database error while retrieving active locations for tables.
 */
export type GetDatasetsError = Error.DatasetStoreError | Error.MetadataDbError

/**
 * The get dataset versions endpoint (GET /datasets/{namespace}/{name}/versions).
 */
const getDatasetVersions = HttpApiEndpoint.get(
  "getDatasetVersions",
)`/datasets/${datasetNamespace}/${datasetName}/versions`
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.DatasetVersionsResponse)

/**
 * Error type for the `getDatasetVersions` endpoint.
 *
 * - InvalidRequest: Invalid namespace or name in path parameters.
 * - DatasetStoreError: Failed to list version tags from dataset store.
 * - MetadataDbError: Database error while retrieving versions.
 */
export type GetDatasetVersionsError = Error.InvalidRequest | Error.DatasetStoreError | Error.MetadataDbError

/**
 * The get dataset by revision endpoint (GET /datasets/{namespace}/{name}/versions/{revision}).
 */
const getDatasetVersion = HttpApiEndpoint.get(
  "getDatasetVersion",
)`/datasets/${datasetNamespace}/${datasetName}/versions/${datasetRevision}`
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetNotFound)
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.DatasetVersionInfo)

/**
 * Error type for the `getDatasetVersion` endpoint.
 *
 * - InvalidRequest: Invalid namespace, name, or revision in path parameters.
 * - DatasetNotFound: The dataset or revision was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 * - MetadataDbError: Database error while retrieving dataset information.
 */
export type GetDatasetVersionError =
  | Error.InvalidRequest
  | Error.DatasetNotFound
  | Error.DatasetStoreError
  | Error.MetadataDbError

/**
 * The deploy dataset endpoint (POST /datasets/{namespace}/{name}/versions/{revision}/deploy).
 */
const deployDataset = HttpApiEndpoint.post(
  "deployDataset",
)`/datasets/${datasetNamespace}/${datasetName}/versions/${datasetRevision}/deploy`
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetNotFound)
  .addError(Error.DatasetStoreError)
  .addError(Error.SchedulerError)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.DeployResponse, { status: 202 })
  .setPayload(Model.DeployRequest)

/**
 * Error type for the `deployDataset` endpoint.
 *
 * - InvalidRequest: Invalid path parameters or request body.
 * - DatasetNotFound: The dataset or revision was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 * - SchedulerError: Failed to schedule the deployment job.
 * - MetadataDbError: Database error while scheduling job.
 */
export type DeployDatasetError =
  | Error.InvalidRequest
  | Error.DatasetNotFound
  | Error.DatasetStoreError
  | Error.SchedulerError
  | Error.MetadataDbError

/**
 * The get dataset manifest endpoint (GET /datasets/{namespace}/{name}/versions/{revision}/manifest).
 */
const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest",
)`/datasets/${datasetNamespace}/${datasetName}/versions/${datasetRevision}/manifest`
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetNotFound)
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Union(Model.DatasetManifest, Model.DatasetRpc))

/**
 * Error type for the `getDatasetManifest` endpoint.
 *
 * - InvalidRequest: Invalid namespace, name, or revision in path parameters.
 * - DatasetNotFound: The dataset, revision, or manifest was not found.
 * - DatasetStoreError: Failed to read manifest from store.
 * - MetadataDbError: Database error while retrieving manifest path.
 */
export type GetDatasetManifestError =
  | Error.InvalidRequest
  | Error.DatasetNotFound
  | Error.DatasetStoreError
  | Error.MetadataDbError

/**
 * The get jobs endpoint (GET /jobs).
 */
const getJobs = HttpApiEndpoint.get("getJobs")`/jobs`
  .addError(Error.InvalidQueryParameters)
  .addError(Error.LimitTooLarge)
  .addError(Error.LimitInvalid)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.JobsResponse)
  .setUrlParams(
    Schema.Struct({
      limit: Schema.optional(Schema.NumberFromString),
      lastJobId: Schema.optional(Model.JobIdParam).pipe(Schema.fromKey("last_job_id")),
    }),
  )

/**
 * Error type for the `getJobs` endpoint.
 */
export type GetJobsError =
  | Error.InvalidQueryParameters
  | Error.LimitTooLarge
  | Error.LimitInvalid
  | Error.MetadataDbError

/**
 * The get job by ID endpoint (GET /jobs/{jobId}).
 */
const getJobById = HttpApiEndpoint.get("getJobById")`/jobs/${jobId}`
  .addError(Error.InvalidJobId)
  .addError(Error.JobNotFound)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.JobInfo)

/**
 * Error type for the `getJobById` endpoint.
 *
 * - InvalidJobId: The provided ID is not a valid job identifier.
 * - JobNotFound: No job exists with the given ID.
 * - MetadataDbError: Internal database error occurred.
 */
export type GetJobByIdError = Error.InvalidJobId | Error.JobNotFound | Error.MetadataDbError

/**
 * The delete all jobs endpoint (DELETE /jobs).
 */
const deleteAllJobs = HttpApiEndpoint.del("deleteAllJobs")`/jobs`
  .addError(Error.InvalidQueryParam)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Void, { status: 204 })
  .setUrlParams(
    Schema.Struct({
      status: Model.JobStatusParam,
    }),
  )

/**
 * Error type for the `deleteAllJobs` endpoint.
 */
export type DeleteAllJobsError = Error.InvalidQueryParam | Error.MetadataDbError

/**
 * The delete job by ID endpoint (DELETE /jobs/{jobId}).
 */
const deleteJobById = HttpApiEndpoint.del("deleteJobById")`/jobs/${jobId}`
  .addError(Error.InvalidJobId)
  .addError(Error.JobNotFound)
  .addError(Error.JobConflict)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Void, { status: 204 })

/**
 * Error type for the `deleteJobById` endpoint.
 *
 * - InvalidJobId: The provided ID is not a valid job identifier.
 * - JobNotFound: No job exists with the given ID.
 * - JobConflict: Job exists but is not in a terminal state.
 * - MetadataDbError: Internal database error occurred.
 */
export type DeleteJobByIdError = Error.InvalidJobId | Error.JobNotFound | Error.JobConflict | Error.MetadataDbError

/**
 * The stop job endpoint (PUT /jobs/{jobId}/stop).
 */
const stopJob = HttpApiEndpoint.put("stopJob")`/jobs/${jobId}/stop`
  .addError(Error.InvalidJobId)
  .addError(Error.JobNotFound)
  .addError(Error.JobConflict)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Void, { status: 204 })

/**
 * Error type for the `stopJob` endpoint.
 *
 * - InvalidJobId: The provided ID is not a valid job identifier.
 * - JobNotFound: No job exists with the given ID.
 * - JobConflict: Job is in a state that cannot be stopped.
 * - MetadataDbError: Internal database error occurred.
 */
export type StopJobError = Error.InvalidJobId | Error.JobNotFound | Error.JobConflict | Error.MetadataDbError

/**
 * The get locations endpoint (GET /locations).
 */
const getLocations = HttpApiEndpoint.get("getLocations")`/locations`
  .addError(Error.InvalidQueryParameters)
  .addError(Error.LimitTooLarge)
  .addError(Error.LimitInvalid)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.LocationsResponse)
  .setUrlParams(
    Schema.Struct({
      limit: Schema.optional(Schema.NumberFromString),
      lastLocationId: Schema.optional(Model.LocationIdParam).pipe(Schema.fromKey("last_location_id")),
    }),
  )

/**
 * Error type for the `getLocations` endpoint.
 */
export type GetLocationsError =
  | Error.InvalidQueryParameters
  | Error.LimitTooLarge
  | Error.LimitInvalid
  | Error.MetadataDbError

/**
 * The get location by ID endpoint (GET /locations/{locationId}).
 */
const getLocationById = HttpApiEndpoint.get("getLocationById")`/locations/${locationId}`
  .addError(Error.InvalidLocationId)
  .addError(Error.LocationNotFound)
  .addError(Error.MetadataDbError)
  .addSuccess(Model.LocationInfo)

/**
 * Error type for the `getLocationById` endpoint.
 *
 * - InvalidLocationId: The provided ID is not a valid location identifier.
 * - LocationNotFound: No location exists with the given ID.
 * - MetadataDbError: Internal database error occurred.
 */
export type GetLocationByIdError = Error.InvalidLocationId | Error.LocationNotFound | Error.MetadataDbError

/**
 * The delete location by ID endpoint (DELETE /locations/{locationId}).
 */
const deleteLocationById = HttpApiEndpoint.del("deleteLocationById")`/locations/${locationId}`
  .addError(Error.InvalidLocationId)
  .addError(Error.InvalidQueryParameters)
  .addError(Error.LocationNotFound)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Void, { status: 204 })
  .setUrlParams(
    Schema.Struct({
      force: Schema.optional(Schema.BooleanFromString),
    }),
  )

/**
 * Error type for the `deleteLocationById` endpoint.
 *
 * - InvalidLocationId: The provided ID is not a valid location identifier.
 * - InvalidQueryParameters: Invalid query parameters.
 * - LocationNotFound: No location exists with the given ID.
 * - MetadataDbError: Internal database error occurred.
 *
 * Note: LocationConflict error (409) exists in the API but is not yet defined in the TypeScript error types.
 */
export type DeleteLocationByIdError =
  | Error.InvalidLocationId
  | Error.InvalidQueryParameters
  | Error.LocationNotFound
  | Error.MetadataDbError

/**
 * The output schema endpoint (POST /schema).
 */
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema")`/schema`
  .addError(Error.DatasetStoreError)
  .addError(Error.PlanningError)
  .addError(Error.CatalogForSqlError)
  .addSuccess(Model.OutputSchema)
  .setPayload(
    Schema.Struct({
      sqlQuery: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("sql_query")),
      isSqlDataset: Schema.Boolean.pipe(Schema.optional, Schema.fromKey("is_sql_dataset")),
    }),
  )

/**
 * Error type for the `getOutputSchema` endpoint.
 *
 * - DatasetStoreError: Failure in dataset storage operations.
 * - PlanningError: Query planning or schema inference failure.
 * - CatalogForSqlError: Failed to build catalog for SQL query.
 */
export type GetOutputSchemaError = Error.DatasetStoreError | Error.PlanningError | Error.CatalogForSqlError

/**
 * The api group for the dataset endpoints.
 */
export class DatasetGroup extends HttpApiGroup.make("dataset")
  .add(registerDataset)
  .add(getDatasets)
  .add(getDatasetVersions)
  .add(getDatasetVersion)
  .add(deployDataset)
  .add(getDatasetManifest)
{}

/**
 * The api group for the job endpoints.
 */
export class JobGroup extends HttpApiGroup.make("job")
  .add(getJobs)
  .add(getJobById)
  .add(deleteAllJobs)
  .add(deleteJobById)
  .add(stopJob)
{}

/**
 * The api group for the location endpoints.
 */
export class LocationGroup extends HttpApiGroup.make("location")
  .add(getLocations)
  .add(getLocationById)
  .add(deleteLocationById)
{}

/**
 * The api group for the schema endpoints.
 */
export class SchemaGroup extends HttpApiGroup.make("schema")
  .add(getOutputSchema)
{}

/**
 * The api definition for the admin api.
 */
export class Api extends HttpApi.make("admin")
  .add(DatasetGroup)
  .add(JobGroup)
  .add(LocationGroup)
  .add(SchemaGroup)
{}

/**
 * Options for dumping a dataset.
 */
export interface DumpDatasetOptions {
  /**
   * The version of the dataset to dump.
   */
  readonly version?: string | undefined
  /**
   * The block up to which to dump.
   */
  readonly endBlock?: number | undefined
}

/**
 * Options for schema retrieval.
 */
export interface GetSchemaOptions {
  /**
   * Whether this is a sql dataset.
   *
   * @default true
   */
  readonly isSqlDataset?: boolean | undefined
}

/**
 * Service definition for the admin api.
 */
export class Admin extends Context.Tag("Amp/Admin")<Admin, {
  /**
   * Register a dataset manifest.
   *
   * @param name The name of the dataset to register.
   * @param version The version of the dataset to register.
   * @param manifest The dataset manifest to register.
   * @return Whether the registration was successful.
   */
  readonly registerDataset: (
    name: string,
    version: string,
    manifest: Model.DatasetManifest | Model.DatasetRpc,
  ) => Effect.Effect<void, HttpClientError.HttpClientError | RegisterDatasetError>

  /**
   * Get all datasets.
   *
   * @return The list of all datasets.
   */
  readonly getDatasets: () => Effect.Effect<Model.DatasetsResponse, HttpClientError.HttpClientError | GetDatasetsError>

  /**
   * Get all versions of a specific dataset.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @return The list of all dataset versions.
   */
  readonly getDatasetVersions: (
    namespace: string,
    name: string,
  ) => Effect.Effect<Model.DatasetVersionsResponse, HttpClientError.HttpClientError | GetDatasetVersionsError>

  /**
   * Get a specific dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @param revision The version/revision of the dataset.
   * @return The dataset version information.
   */
  readonly getDatasetVersion: (
    namespace: string,
    name: string,
    revision: string,
  ) => Effect.Effect<Model.DatasetVersionInfo, HttpClientError.HttpClientError | GetDatasetVersionError>

  /**
   * Deploy a dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset to deploy.
   * @param revision The version/revision to deploy.
   * @param options The deployment options.
   * @return The deployment response with job ID.
   */
  readonly deployDataset: (
    namespace: string,
    name: string,
    revision: string,
    options?: {
      endBlock?: string | null | undefined
      parallelism?: number | undefined
    } | undefined,
  ) => Effect.Effect<Model.DeployResponse, HttpClientError.HttpClientError | DeployDatasetError>

  /**
   * Get the manifest for a dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @param revision The version/revision of the dataset.
   * @return The dataset manifest.
   */
  readonly getDatasetManifest: (
    namespace: string,
    name: string,
    revision: string,
  ) => Effect.Effect<any, HttpClientError.HttpClientError | GetDatasetManifestError>

  /**
   * Get all jobs with pagination.
   *
   * @param options The pagination options.
   * @return The paginated jobs response.
   */
  readonly getJobs: (options?: {
    limit?: number | undefined
    lastJobId?: number | undefined
  }) => Effect.Effect<Model.JobsResponse, HttpClientError.HttpClientError | GetJobsError>

  /**
   * Get a job by ID.
   *
   * @param jobId The ID of the job to get.
   * @return The job information.
   */
  readonly getJobById: (
    jobId: number,
  ) => Effect.Effect<Model.JobInfo, HttpClientError.HttpClientError | GetJobByIdError>

  /**
   * Delete all jobs by status filter.
   *
   * @param status The status filter for jobs to delete ("terminal", "complete", "stopped", "error")
   */
  readonly deleteAllJobs: (
    status: "terminal" | "complete" | "stopped" | "error",
  ) => Effect.Effect<void, HttpClientError.HttpClientError | DeleteAllJobsError>

  /**
   * Delete a job by ID.
   *
   * @param jobId The ID of the job to delete.
   */
  readonly deleteJobById: (
    jobId: number,
  ) => Effect.Effect<void, HttpClientError.HttpClientError | DeleteJobByIdError>

  /**
   * Stop a job.
   *
   * @param jobId The ID of the job to stop.
   */
  readonly stopJob: (jobId: number) => Effect.Effect<void, HttpClientError.HttpClientError | StopJobError>

  /**
   * Get all locations with pagination.
   *
   * @param options The pagination options.
   * @return The paginated locations response.
   */
  readonly getLocations: (options?: {
    limit?: number | undefined
    lastLocationId?: number | undefined
  }) => Effect.Effect<Model.LocationsResponse, HttpClientError.HttpClientError | GetLocationsError>

  /**
   * Get a location by ID.
   *
   * @param locationId The ID of the location to get.
   * @return The location information.
   */
  readonly getLocationById: (
    locationId: number,
  ) => Effect.Effect<Model.LocationInfo, HttpClientError.HttpClientError | GetLocationByIdError>

  /**
   * Delete a location by ID.
   *
   * @param locationId The ID of the location to delete.
   * @param options Optional parameters including force flag.
   */
  readonly deleteLocationById: (
    locationId: number,
    options?: {
      force?: boolean | undefined
    },
  ) => Effect.Effect<void, HttpClientError.HttpClientError | DeleteLocationByIdError>

  /**
   * Gets the schema of a dataset.
   *
   * @param sql - The SQL query to get the schema for.
   * @param options - Options for the schema retrieval.
   * @returns An effect that resolves to the table schema.
   */
  readonly getOutputSchema: (
    sql: string,
    options?: GetSchemaOptions,
  ) => Effect.Effect<Model.OutputSchema, HttpClientError.HttpClientError | GetOutputSchemaError>
}>() {}

/**
 * Creates an admin api service instance.
 *
 * @param url - The url of the admin api service.
 * @returns An admin api service instance.
 */
export const make = Effect.fn(function*(url: string) {
  const client = yield* HttpApiClient.make(Api, {
    baseUrl: url,
  })

  const registerDataset = Effect.fn("registerDataset")(
    function*(name: string, version: string, manifest: Model.DatasetManifest | Model.DatasetRpc) {
      const request = client.dataset.registerDataset({
        payload: {
          // TODO: Extract namespace from dataset config's owner field instead of using placeholder
          namespace: "_",
          name,
          version,
          manifest,
        },
      })

      const result = yield* request.pipe(
        Effect.catchTags({
          HttpApiDecodeError: Effect.die,
          ParseError: Effect.die,
        }),
      )

      return result
    },
  )

  const getDatasetVersions = Effect.fn("getDatasetVersions")(function*(namespace: string, name: string) {
    const result = yield* client.dataset.getDatasetVersions({
      path: {
        namespace,
        name,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasetVersion = Effect.fn("getDatasetVersion")(function*(
    namespace: string,
    name: string,
    revision: string,
  ) {
    const result = yield* client.dataset.getDatasetVersion({
      path: {
        namespace,
        name,
        revision,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const deployDataset = Effect.fn("deployDataset")(function*(
    namespace: string,
    name: string,
    revision: string,
    options?: {
      endBlock?: string | null | undefined
      parallelism?: number | undefined
    },
  ) {
    const result = yield* client.dataset.deployDataset({
      path: {
        namespace,
        name,
        revision,
      },
      payload: {
        endBlock: options?.endBlock,
        parallelism: options?.parallelism,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasetManifest = Effect.fn("getDatasetManifest")(function*(
    namespace: string,
    name: string,
    revision: string,
  ) {
    const result = yield* client.dataset.getDatasetManifest({
      path: {
        namespace,
        name,
        revision,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasets = Effect.fn("getDatasets")(function*() {
    const result = yield* client.dataset.getDatasets({}).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getJobs = Effect.fn("getJobs")(
    function*(options?: {
      limit?: number | undefined
      lastJobId?: number | undefined
    }) {
      const result = yield* client.job.getJobs({
        urlParams: {
          limit: options?.limit,
          lastJobId: options?.lastJobId,
        },
      }).pipe(
        Effect.catchTags({
          HttpApiDecodeError: Effect.die,
          ParseError: Effect.die,
        }),
      )

      return result
    },
  )

  const getJobById = Effect.fn("getJobById")(function*(jobId: number) {
    const result = yield* client.job.getJobById({
      path: {
        jobId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const deleteAllJobs = Effect.fn("deleteAllJobs")(function*(status: "terminal" | "complete" | "stopped" | "error") {
    yield* client.job.deleteAllJobs({
      urlParams: {
        status,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )
  })

  const deleteJobById = Effect.fn("deleteJobById")(function*(jobId: number) {
    yield* client.job.deleteJobById({
      path: {
        jobId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )
  })

  const stopJob = Effect.fn("stopJob")(function*(jobId: number) {
    yield* client.job.stopJob({
      path: {
        jobId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )
  })

  const getLocations = Effect.fn("getLocations")(function*(options?: {
    limit?: number | undefined
    lastLocationId?: number | undefined
  }) {
    const result = yield* client.location.getLocations({
      urlParams: {
        limit: options?.limit,
        lastLocationId: options?.lastLocationId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getLocationById = Effect.fn("getLocationById")(function*(locationId: number) {
    const result = yield* client.location.getLocationById({
      path: {
        locationId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const deleteLocationById = Effect.fn("deleteLocationById")(function*(
    locationId: number,
    options?: {
      force?: boolean | undefined
    },
  ) {
    yield* client.location.deleteLocationById({
      path: {
        locationId,
      },
      urlParams: {
        force: options?.force,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )
  })

  const getOutputSchema = Effect.fn("getOutputSchema")(function*(sql: string, options?: GetSchemaOptions) {
    const request = client.schema.getOutputSchema({
      payload: {
        sqlQuery: sql,
        isSqlDataset: options?.isSqlDataset ?? true,
      },
    })

    const result = yield* request.pipe(
      Effect.catchTags({
        ParseError: Effect.die,
        HttpApiDecodeError: Effect.die,
      }),
    )

    return result
  })

  return {
    registerDataset,
    getDatasets,
    getDatasetVersions,
    getDatasetVersion,
    deployDataset,
    getDatasetManifest,
    getJobs,
    getJobById,
    deleteAllJobs,
    deleteJobById,
    stopJob,
    getLocations,
    getLocationById,
    deleteLocationById,
    getOutputSchema,
  }
})

/**
 * Creates a layer for the admin api service.
 *
 * @param url - The url of the admin api service.
 * @returns A layer for the admin api service.
 */
export const layer = (url: string) => make(url).pipe(Layer.effect(Admin), Layer.provide(FetchHttpClient.layer))
