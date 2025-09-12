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
 * The dataset name parameter (GET /datasets/{name}).
 */
const datasetName = HttpApiSchema.param("name", Model.DatasetName)

/**
 * The dataset version parameter (GET /datasets/{name}/versions/{version}).
 */
const datasetVersion = HttpApiSchema.param("version", Model.DatasetVersion)

/**
 * The job ID parameter (GET /jobs/{jobId}, DELETE /jobs/{jobId}, PUT /jobs/{jobId}/stop).
 */
const jobId = HttpApiSchema.param("jobId", Model.JobIdParam)

/**
 * The location ID parameter (GET /locations/{locationId}, DELETE /locations/{locationId}).
 */
const locationId = HttpApiSchema.param("locationId", Model.LocationIdParam)

/**
 * The dump dataset endpoint (POST /datasets/{name}/dump).
 */
const dumpDataset = HttpApiEndpoint.post("dumpDataset")`/datasets/${datasetName}/dump`
  .addError(Error.DatasetNotFound)
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidSelector)
  .addError(Error.UnexpectedJobStatus)
  .addError(Error.SchedulerError)
  .addError(Error.MetadataDbError)
  .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
  .setPayload(
    Schema.Struct({
      endBlock: Schema.Number.pipe(Schema.optional, Schema.fromKey("end_block")),
    }),
  )

/**
 * The dump dataset with version endpoint (POST /datasets/{name}/versions/{version}/dump).
 */
const dumpDatasetVersion = HttpApiEndpoint.post(
  "dumpDatasetVersion",
)`/datasets/${datasetName}/versions/${datasetVersion}/dump`
  .addError(Error.DatasetNotFound)
  .addError(Error.InvalidRequest)
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidSelector)
  .addError(Error.UnexpectedJobStatus)
  .addError(Error.SchedulerError)
  .addError(Error.MetadataDbError)
  .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
  .setPayload(
    Schema.Struct({
      endBlock: Schema.Number.pipe(Schema.optional, Schema.fromKey("end_block")),
    }),
  )

/**
 * Error type for the `dumpDataset` endpoint.
 *
 * - InvalidRequest: Invalid request parameters.
 * - DatasetStoreError: Failed to load dataset from store.
 * - InvalidSelector: The dataset selector is invalid.
 * - UnexpectedJobStatus: The job status is unexpected.
 * - SchedulerError: Failed to schedule the dump job.
 * - MetadataDbError: Database error while polling job status.
 */
export type DumpDatasetError =
  | Error.DatasetNotFound
  | Error.InvalidRequest
  | Error.DatasetStoreError
  | Error.InvalidSelector
  | Error.UnexpectedJobStatus
  | Error.SchedulerError
  | Error.MetadataDbError

/**
 * The register dataset endpoint (POST /datasets).
 */
const registerDataset = HttpApiEndpoint.post("registerDataset")`/datasets`
  .addError(Error.InvalidRequest)
  .addError(Error.InvalidManifest)
  .addError(Error.ManifestValidationError)
  .addError(Error.ManifestRegistrationError)
  .addError(Error.ManifestRequired)
  .addError(Error.DatasetAlreadyExists)
  .addError(Error.SchedulerError)
  .addError(Error.DatasetDefStoreError)
  .addError(Error.DatasetStoreError)
  .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
  .setPayload(
    Schema.Struct({
      name: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("name")),
      version: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("version")),
      manifest: Schema.parseJson(Schema.Union(Model.DatasetManifest, Model.DatasetRpc)).pipe(Schema.optional),
    }),
  )

/**
 * Error type for the `registerDataset` endpoint.
 *
 * - InvalidRequest: Invalid request parameters or dataset name format.
 * - InvalidManifest: The manifest is semantically invalid.
 * - ManifestValidationError: Manifest name/version doesn't match request parameters.
 * - ManifestRegistrationError: Failed to register manifest in system.
 * - ManifestRequired: Dataset not found and manifest not provided.
 * - DatasetAlreadyExists: Dataset exists and manifest provided (conflict).
 * - SchedulerError: Failure in scheduling the dataset dump after registration.
 * - DatasetDefStoreError: Failure in dataset definition store operations.
 * - DatasetStoreError: Failed to load dataset from store.
 */
export type RegisterDatasetError =
  | Error.InvalidRequest
  | Error.InvalidManifest
  | Error.ManifestValidationError
  | Error.ManifestRegistrationError
  | Error.ManifestRequired
  | Error.DatasetAlreadyExists
  | Error.SchedulerError
  | Error.DatasetDefStoreError
  | Error.DatasetStoreError

/**
 * The get datasets endpoint (GET /datasets).
 */
const getDatasets = HttpApiEndpoint.get("getDatasets")`/datasets`
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addSuccess(
    Schema.Struct({
      datasets: Schema.Array(Model.DatasetInfo).pipe(Schema.mutable),
    }),
  )

/**
 * Error type for the `getDatasets` endpoint.
 *
 * - DatasetStoreError: Failed to retrieve datasets from the dataset store.
 * - MetadataDbError: Database error while retrieving active locations for tables.
 */
export type GetDatasetsError = Error.DatasetStoreError | Error.MetadataDbError

/**
 * The get dataset by name endpoint (GET /datasets/{name}).
 */
const getDataset = HttpApiEndpoint.get("getDataset")`/datasets/${datasetName}`
  .addError(Error.InvalidSelector)
  .addError(Error.DatasetNotFound)
  .addError(Error.DatasetStoreError)
  .addSuccess(Model.DatasetInfo)

/**
 * Error type for the `getDataset` endpoint.
 *
 * - InvalidSelector: The dataset selector is invalid.
 * - DatasetNotFound: The dataset was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 */
export type GetDatasetError = Error.InvalidSelector | Error.DatasetNotFound | Error.DatasetStoreError

/**
 * The get dataset by name and version endpoint (GET /datasets/{name}/versions/{version}).
 */
const getDatasetVersion = HttpApiEndpoint.get(
  "getDatasetVersion",
)`/datasets/${datasetName}/versions/${datasetVersion}`
  .addError(Error.InvalidSelector)
  .addError(Error.DatasetNotFound)
  .addError(Error.DatasetStoreError)
  .addSuccess(Model.DatasetInfo)

/**
 * Error type for the `getDatasetVersion` endpoint.
 *
 * - InvalidSelector: The dataset selector is invalid.
 * - DatasetNotFound: The dataset was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 */
export type GetDatasetVersionError = Error.InvalidSelector | Error.DatasetNotFound | Error.DatasetStoreError

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
  .addError(Error.LocationNotFound)
  .addError(Error.MetadataDbError)
  .addSuccess(Schema.Void, { status: 204 })

/**
 * Error type for the `deleteLocationById` endpoint.
 *
 * - InvalidLocationId: The provided ID is not a valid location identifier.
 * - LocationNotFound: No location exists with the given ID.
 * - MetadataDbError: Internal database error occurred.
 */
export type DeleteLocationByIdError = Error.InvalidLocationId | Error.LocationNotFound | Error.MetadataDbError

/**
 * The api group for the dataset endpoints.
 */
export class DatasetGroup extends HttpApiGroup.make("dataset")
  .add(registerDataset)
  .add(dumpDataset)
  .add(dumpDatasetVersion)
  .add(getDataset)
  .add(getDatasetVersion)
  .add(getDatasets)
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
 * The api definition for the admin api.
 */
export class Api extends HttpApi.make("admin")
  .add(DatasetGroup)
  .add(JobGroup)
  .add(LocationGroup)
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
 * Service definition for the admin api.
 */
export class Admin extends Context.Tag("Nozzle/Admin")<Admin, {
  /**
   * Register a dataset manifest.
   *
   * @param name The name of the dataset to register.
   * @param version The version of the dataset to register.
   * @return Whether the registration was successful.
   */
  readonly registerDataset: (
    name: string,
    version: string,
    manifest?: Model.DatasetManifest | Model.DatasetRpc,
  ) => Effect.Effect<string, HttpClientError.HttpClientError | RegisterDatasetError>

  /**
   * Dump a dataset.
   *
   * @param name The name of the dataset to dump.
   * @param options The options for dumping.
   * @return Whether the dump or dump scheduling was successful.
   */
  readonly dumpDataset: (
    name: string,
    options?: {
      endBlock?: number | undefined
    } | undefined,
  ) => Effect.Effect<string, HttpClientError.HttpClientError | DumpDatasetError>

  /**
   * Dump a dataset with a specific version.
   *
   * @param name The name of the dataset to dump.
   * @param version The version of the dataset to dump.
   * @param options The options for dumping.
   * @return Whether the dump or dump scheduling was successful.
   */
  readonly dumpDatasetVersion: (
    name: string,
    version: string,
    options?: {
      endBlock?: number | undefined
    } | undefined,
  ) => Effect.Effect<string, HttpClientError.HttpClientError | DumpDatasetError>

  /**
   * Get a dataset by name.
   *
   * @param name The name of the dataset to get.
   * @return The dataset information.
   */
  readonly getDataset: (
    name: string,
  ) => Effect.Effect<Model.DatasetInfo, HttpClientError.HttpClientError | GetDatasetError>

  /**
   * Get a dataset by name and version.
   *
   * @param name The name of the dataset to get.
   * @param version The version of the dataset to get.
   * @return The dataset information.
   */
  readonly getDatasetVersion: (
    name: string,
    version: string,
  ) => Effect.Effect<Model.DatasetInfo, HttpClientError.HttpClientError | GetDatasetVersionError>

  /**
   * Get all datasets.
   *
   * @return The list of datasets.
   */
  readonly getDatasets: () => Effect.Effect<
    Array<Model.DatasetInfo>,
    HttpClientError.HttpClientError | GetDatasetsError
  >

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
   */
  readonly deleteLocationById: (
    locationId: number,
  ) => Effect.Effect<void, HttpClientError.HttpClientError | DeleteLocationByIdError>
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
    function*(name: string, version: string, manifest?: Model.DatasetManifest | Model.DatasetRpc) {
      const request = client.dataset.registerDataset({
        payload: {
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

  const dumpDataset = Effect.fn("dumpDataset")(function*(
    name: string,
    options?: {
      endBlock?: number | undefined
    },
  ) {
    const request = client.dataset.dumpDataset({
      path: {
        name,
      },
      payload: {
        endBlock: options?.endBlock,
      },
    })

    const result = yield* request.pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const dumpDatasetVersion = Effect.fn("dumpDatasetVersion")(function*(
    name: string,
    version: string,
    options?: {
      endBlock?: number | undefined
    },
  ) {
    const request = client.dataset.dumpDatasetVersion({
      path: {
        name,
        version,
      },
      payload: {
        endBlock: options?.endBlock,
      },
    })

    const result = yield* request.pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDataset = Effect.fn("getDataset")(function*(name: string) {
    const request = client.dataset.getDataset({
      path: {
        name,
      },
    })

    const result = yield* request.pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasetVersion = Effect.fn("getDatasetVersion")(function*(name: string, version: string) {
    const request = client.dataset.getDatasetVersion({
      path: {
        name,
        version,
      },
    })

    const result = yield* request.pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasets = Effect.fn("getDatasets")(function*() {
    const result = yield* client.dataset.getDatasets().pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result.datasets
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

  const deleteLocationById = Effect.fn("deleteLocationById")(function*(locationId: number) {
    yield* client.location.deleteLocationById({
      path: {
        locationId,
      },
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )
  })

  return {
    dumpDataset,
    dumpDatasetVersion,
    registerDataset,
    getDataset,
    getDatasetVersion,
    getDatasets,
    getJobs,
    getJobById,
    deleteAllJobs,
    deleteJobById,
    stopJob,
    getLocations,
    getLocationById,
    deleteLocationById,
  }
})

/**
 * Creates a layer for the admin api service.
 *
 * @param url - The url of the admin api service.
 * @returns A layer for the admin api service.
 */
export const layer = (url: string) => make(url).pipe(Layer.effect(Admin), Layer.provide(FetchHttpClient.layer))
