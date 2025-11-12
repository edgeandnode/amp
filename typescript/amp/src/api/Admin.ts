import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiClient from "@effect/platform/HttpApiClient"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Model from "../Model.ts"
import * as Error from "./Error.ts"

const OptionalAuthorizationHeaderSchema = Schema.Struct({
  Authorization: Schema.optional(
    Schema.String.pipe(
      Schema.filter((val) => val.startsWith("Bearer ")),
    ),
  ),
})

/**
 * The dataset namespace parameter.
 */
const datasetNamespace = HttpApiSchema.param("namespace", Model.DatasetNamespace)

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
 * The register dataset endpoint (POST /datasets).
 */
const registerDataset = HttpApiEndpoint.post("registerDataset")`/datasets`
  .addError(Error.InvalidPayloadFormat)
  .addError(Error.InvalidManifest)
  .addError(Error.DependencyValidationError)
  .addError(Error.UnsupportedDatasetKind)
  .addError(Error.ManifestRegistrationError)
  .addError(Error.ManifestLinkingError)
  .addError(Error.VersionTaggingError)
  .addError(Error.StoreError)
  .addError(Error.ManifestNotFound)
  .addError(HttpApiError.Unauthorized)
  .addSuccess(Schema.Void, { status: 201 })
  .setHeaders(OptionalAuthorizationHeaderSchema)
  .setPayload(
    Schema.Struct({
      namespace: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("namespace")),
      name: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("name")),
      version: Schema.optional(Schema.String).pipe(Schema.fromKey("version")),
      manifest: Model.DatasetManifest,
    }),
  )

/**
 * Error type for the `registerDataset` endpoint.
 *
 * - InvalidPayloadFormat: Request JSON is malformed or invalid.
 * - InvalidManifest: Manifest JSON is malformed or structurally invalid.
 * - DependencyValidationError: SQL queries reference undeclared dependencies.
 * - UnsupportedDatasetKind: Dataset kind is not supported.
 * - ManifestRegistrationError: Failed to register manifest in system.
 * - ManifestLinkingError: Failed to link manifest to dataset.
 * - VersionTaggingError: Failed to tag version for the dataset.
 * - StoreError: Dataset store operation error.
 * - ManifestNotFound: Manifest hash provided but manifest doesn't exist.
 * - HttpApiError.Unauthorized: If registering with the public cluster and the request is not authorized
 */
export type RegisterDatasetError =
  | Error.InvalidPayloadFormat
  | Error.InvalidManifest
  | Error.DependencyValidationError
  | Error.UnsupportedDatasetKind
  | Error.ManifestRegistrationError
  | Error.ManifestLinkingError
  | Error.VersionTaggingError
  | Error.StoreError
  | Error.ManifestNotFound
  | HttpApiError.Unauthorized

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
  .addError(HttpApiError.Unauthorized)
  .addSuccess(Model.DeployResponse, { status: 202 })
  .setHeaders(OptionalAuthorizationHeaderSchema)
  .setPayload(Model.DeployRequest)

/**
 * Error type for the `deployDataset` endpoint.
 *
 * - InvalidRequest: Invalid path parameters or request body.
 * - DatasetNotFound: The dataset or revision was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 * - SchedulerError: Failed to schedule the deployment job.
 * - MetadataDbError: Database error while scheduling job.
 * - HttpApiError.Unauthorized: If deploying to the public cluster and the request is not authorized
 */
export type DeployDatasetError =
  | Error.InvalidRequest
  | Error.DatasetNotFound
  | Error.DatasetStoreError
  | Error.SchedulerError
  | Error.MetadataDbError
  | HttpApiError.Unauthorized

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
  .addSuccess(Model.DatasetManifest)

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
 * The output schema endpoint (POST /schema).
 */
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema")`/schema`
  .addError(Error.InvalidPayloadFormat)
  .addError(Error.EmptyTablesAndFunctions)
  .addError(Error.InvalidTableSql)
  .addError(Error.TableReferenceResolution)
  .addError(Error.FunctionReferenceResolution)
  .addError(Error.DependencyNotFound)
  .addError(Error.DependencyResolution)
  .addError(Error.CatalogQualifiedTable)
  .addError(Error.UnqualifiedTable)
  .addError(Error.InvalidTableName)
  .addError(Error.DatasetNotFound)
  .addError(Error.GetDatasetError)
  .addError(Error.EthCallUdfCreationError)
  .addError(Error.TableNotFoundInDataset)
  .addError(Error.FunctionNotFoundInDataset)
  .addError(Error.EthCallNotAvailable)
  .addError(Error.DependencyAliasNotFound)
  .addError(Error.SchemaInference)
  .addError(HttpApiError.Unauthorized)
  .addSuccess(Model.SchemaResponse)
  .setPayload(Model.SchemaRequest)
  .setHeaders(OptionalAuthorizationHeaderSchema)

/**
 * Error type for the `getOutputSchema` endpoint.
 *
 * - InvalidPayloadFormat: Request JSON is malformed or missing required fields.
 * - EmptyTablesAndFunctions: No tables or functions provided (at least one required).
 * - InvalidTableSql: SQL query has invalid syntax.
 * - TableReferenceResolution: Failed to resolve table references in SQL.
 * - FunctionReferenceResolution: Failed to resolve function references in SQL.
 * - DependencyNotFound: Referenced dependency does not exist.
 * - DependencyResolution: Failed to resolve dependency to hash.
 * - CatalogQualifiedTable: Table reference includes catalog qualifier (not supported).
 * - UnqualifiedTable: Table reference is not qualified with dataset.
 * - InvalidTableName: Table name does not conform to SQL identifier rules.
 * - DatasetNotFound: Referenced dataset does not exist in the store.
 * - GetDatasetError: Failed to retrieve dataset from store.
 * - EthCallUdfCreationError: Failed to create ETH call UDF.
 * - TableNotFoundInDataset: Referenced table does not exist in dataset.
 * - FunctionNotFoundInDataset: Referenced function does not exist in dataset.
 * - EthCallNotAvailable: eth_call function not available for dataset.
 * - DependencyAliasNotFound: Table or function reference uses undefined alias.
 * - SchemaInference: Failed to infer schema for table.
 * - HttpApiError.Unauthorized: If reading the output schema on the public cluster is unauthorized (missing Authorization bearer token, or invalid token)
 */
export type GetOutputSchemaError =
  | Error.InvalidPayloadFormat
  | Error.EmptyTablesAndFunctions
  | Error.InvalidTableSql
  | Error.TableReferenceResolution
  | Error.FunctionReferenceResolution
  | Error.DependencyNotFound
  | Error.DependencyResolution
  | Error.CatalogQualifiedTable
  | Error.UnqualifiedTable
  | Error.InvalidTableName
  | Error.DatasetNotFound
  | Error.GetDatasetError
  | Error.EthCallUdfCreationError
  | Error.TableNotFoundInDataset
  | Error.FunctionNotFoundInDataset
  | Error.EthCallNotAvailable
  | Error.DependencyAliasNotFound
  | Error.SchemaInference
  | HttpApiError.Unauthorized

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
export class JobGroup extends HttpApiGroup.make("job").add(getJobById) {}

/**
 * The api group for the schema endpoints.
 */
export class SchemaGroup extends HttpApiGroup.make("schema").add(getOutputSchema) {}

/**
 * The api definition for the admin api.
 */
export class Api extends HttpApi.make("admin")
  .add(DatasetGroup)
  .add(JobGroup)
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
 * Service definition for the admin api.
 */
export class Admin extends Context.Tag("Amp/Admin")<Admin, {
  /**
   * Register a dataset manifest.
   *
   * @param namespace The namespace of the dataset to register.
   * @param name The name of the dataset to register.
   * @param version Optional version of the dataset to register. If omitted, only the "dev" tag is updated.
   * @param manifest The dataset manifest to register.
   * @param bearerToken Optional Authorization Bearer JWT. Required for registering to the test cluster admin-api
   * @return Whether the registration was successful.
   */
  readonly registerDataset: (
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    manifest: Model.DatasetManifest,
    version?: Model.DatasetVersion | undefined,
    bearerToken?: string | undefined,
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
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
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
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
  ) => Effect.Effect<Model.DatasetVersionInfo, HttpClientError.HttpClientError | GetDatasetVersionError>

  /**
   * Deploy a dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset to deploy.
   * @param revision The version/revision to deploy.
   * @param options The deployment options.
   * @param bearerToken Optional Authorization Bearer JWT. Required for registering to the test cluster admin-api
   * @return The deployment response with job ID.
   */
  readonly deployDataset: (
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
    options?: {
      endBlock?: string | null | undefined
      parallelism?: number | undefined
    } | undefined,
    bearerToken?: string | undefined,
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
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
  ) => Effect.Effect<any, HttpClientError.HttpClientError | GetDatasetManifestError>

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
   * Gets the schema of a dataset.
   *
   * @param request - The schema request with tables and dependencies.
   * @param bearerToken Optional Authorization Bearer JWT. Required for reading the output schema from the public cluster admin-api
   * @returns An effect that resolves to the schema response.
   */
  readonly getOutputSchema: (
    request: Model.SchemaRequest,
    bearerToken?: string | undefined,
  ) => Effect.Effect<Model.SchemaResponse, HttpClientError.HttpClientError | GetOutputSchemaError>
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
    function*(
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
      manifest: Model.DatasetManifest,
      version?: Model.DatasetVersion | undefined,
      bearerToken?: string,
    ) {
      const request = client.dataset.registerDataset({
        payload: {
          namespace,
          name,
          manifest,
          version,
        },
        headers: bearerToken != null ? { Authorization: `Bearer ${bearerToken}` } : {},
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

  const getDatasetVersions = Effect.fn("getDatasetVersions")(function*(
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
  ) {
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
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
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
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
    options?: {
      endBlock?: string | null | undefined
      parallelism?: number | undefined
    },
    bearerToken?: string | undefined,
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
      headers: bearerToken != null ? { Authorization: `Bearer ${bearerToken}` } : {},
    }).pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result
  })

  const getDatasetManifest = Effect.fn("getDatasetManifest")(function*(
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    revision: Model.DatasetRevision,
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

  const getOutputSchema = Effect.fn("getOutputSchema")(
    function*(request: Model.SchemaRequest, bearerToken?: string | undefined) {
      const result = yield* client.schema.getOutputSchema({
        payload: request,
        headers: bearerToken != null ? { Authorization: `Bearer ${bearerToken}` } : {},
      }).pipe(
        Effect.catchTags({
          ParseError: Effect.die,
          HttpApiDecodeError: Effect.die,
        }),
      )

      return result
    },
  )

  return {
    registerDataset,
    getDatasets,
    getDatasetVersions,
    getDatasetVersion,
    deployDataset,
    getDatasetManifest,
    getJobById,
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
