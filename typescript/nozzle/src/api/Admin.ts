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
 * The dataset ID parameter (GET /datasets/{datasetId}).
 */
const datasetId = HttpApiSchema.param("datasetId", Model.DatasetName)

/**
 * The dump dataset endpoint (POST /datasets/{datasetId}/dump).
 */
const dumpDataset = HttpApiEndpoint.post("dumpDataset")`/datasets/${datasetId}/dump`
  .addError(Error.InvalidDatasetId)
  .addError(Error.UnexpectedJobStatus)
  .addError(Error.DatasetStoreError)
  .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
  .setPayload(
    Schema.Struct({
      endBlock: Schema.Number.pipe(Schema.propertySignature, Schema.fromKey("end_block")),
      waitForCompletion: Schema.Boolean.pipe(Schema.optional, Schema.fromKey("wait_for_completion")),
    }),
  )

/**
 * Error type for the `dumpDataset` endpoint.
 */
export type DumpDatasetError =
  | Error.DatasetStoreError
  | Error.InvalidDatasetId
  | Error.UnexpectedJobStatus
  | Error.SchedulerError

/**
 * The deploy dataset endpoint (POST /datasets).
 */
const deployDataset = HttpApiEndpoint.post("deployDataset")`/datasets`
  .addError(Error.SchedulerError)
  .addError(Error.DatasetDefStoreError)
  .addError(Error.ManifestParseError)
  .addError(Error.InvalidManifest)
  .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
  .setPayload(
    Schema.Struct({
      datasetName: Schema.String.pipe(Schema.propertySignature, Schema.fromKey("dataset_name")),
      manifest: Schema.parseJson(Schema.Union(Model.DatasetManifest, Model.DatasetRpc)),
    }),
  )

/**
 * Error type for the `deployDataset` endpoint.
 *
 * - SchedulerError: Failure in scheduling the dataset dump after deployment.
 * - DatasetDefStoreError: Failure in dataset definition store operations.
 * - ManifestParseError: Failure in parsing the manifest.
 * - InvalidManifest: The manifest is invalid.
 */
export type DeployDatasetError =
  | Error.SchedulerError
  | Error.DatasetDefStoreError
  | Error.ManifestParseError
  | Error.InvalidManifest

/**
 * The get datasets endpoint (GET /datasets).
 */
const getDatasets = HttpApiEndpoint.get("getDatasets")`/datasets`.addError(Error.MetadataDbError).addSuccess(
  Schema.Struct({
    datasets: Schema.Array(Model.DatasetInfo).pipe(Schema.mutable),
  }),
)

/**
 * Error type for the `getDatasets` endpoint.
 *
 * - MetadataDbError: Failure in metadata database operations.
 */
export type GetDatasetsError = Error.MetadataDbError

/**
 * The get dataset by ID endpoint (GET /datasets/{datasetId}).
 */
const getDatasetById = HttpApiEndpoint.get("getDatasetById")`/datasets/${datasetId}`
  .addError(Error.DatasetNotFound)
  .addError(Error.InvalidDatasetId)
  .addError(Error.UnexpectedJobStatus)
  .addSuccess(Model.DatasetInfo)

/**
 * Error type for the `getDatasetById` endpoint.
 *
 * - DatasetNotFound: The dataset was not found.
 * - InvalidDatasetId: The dataset ID is invalid.
 * - UnexpectedJobStatus: The job status is unexpected.
 */
export type GetDatasetByIdError = Error.DatasetNotFound | Error.InvalidDatasetId | Error.UnexpectedJobStatus

/**
 * The api definition for the admin api.
 */
export class Api extends HttpApi.make("admin").add(
  HttpApiGroup.make("admin", { topLevel: true })
    .add(deployDataset)
    .add(dumpDataset)
    .add(getDatasetById)
    .add(getDatasets),
) {}

/**
 * Options for dumping a dataset.
 */
export interface DumpDatasetOptions {
  /**
   * The block up to which to dump.
   */
  endBlock: number
  /**
   * Whether to wait for the dump to complete before returning.
   *
   * @default false
   */
  waitForCompletion?: boolean
}

/**
 * Service definition for the admin api.
 */
export class Admin extends Context.Tag("Nozzle/Admin")<Admin, {
  /**
   * Deploy a dataset manifest.
   *
   * @param dataset The dataset manifest to deploy.
   * @return Whether the deployment was successful.
   */
  readonly deployDataset: (
    dataset: Model.DatasetManifest | Model.DatasetRpc,
  ) => Effect.Effect<string, HttpClientError.HttpClientError | DeployDatasetError>

  /**
   * Dump a dataset.
   *
   * @param datasetId The ID of the dataset to dump.
   * @param options The options for dumping.
   * @return Whether the dump or dump scheduling was successful.
   */
  readonly dumpDataset: (
    datasetId: string,
    options: DumpDatasetOptions,
  ) => Effect.Effect<string, HttpClientError.HttpClientError | DumpDatasetError>

  /**
   * Get a dataset by ID.
   *
   * @param datasetId The ID of the dataset to get.
   * @return The dataset information.
   */
  readonly getDatasetById: (
    datasetId: string,
  ) => Effect.Effect<Model.DatasetInfo, HttpClientError.HttpClientError | GetDatasetByIdError>

  /**
   * Get all datasets.
   *
   * @return The list of datasets.
   */
  readonly getDatasets: () => Effect.Effect<
    Array<Model.DatasetInfo>,
    HttpClientError.HttpClientError | GetDatasetsError
  >
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

  const deployDataset = Effect.fn("deployDataset")(function*(dataset: Model.DatasetManifest | Model.DatasetRpc) {
    const request = client.deployDataset({
      payload: {
        manifest: dataset,
        datasetName: dataset.name,
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

  const dumpDataset = Effect.fn("dumpDataset")(function*(
    datasetId: string,
    options: {
      endBlock: number
      waitForCompletion?: boolean
    },
  ) {
    const request = client.dumpDataset({
      path: {
        datasetId,
      },
      payload: {
        waitForCompletion: options.waitForCompletion,
        endBlock: options.endBlock,
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

  const getDatasetById = Effect.fn("getDatasetById")(function*(datasetId: string) {
    const request = client.getDatasetById({
      path: {
        datasetId,
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
    const result = yield* client.getDatasets().pipe(
      Effect.catchTags({
        HttpApiDecodeError: Effect.die,
        ParseError: Effect.die,
      }),
    )

    return result.datasets
  })

  return { dumpDataset, deployDataset, getDatasetById, getDatasets }
})

/**
 * Creates a layer for the admin api service.
 *
 * @param url - The url of the admin api service.
 * @returns A layer for the admin api service.
 */
export const layer = (url: string) => make(url).pipe(Layer.effect(Admin), Layer.provide(FetchHttpClient.layer))
