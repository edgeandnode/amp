import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiClient from "@effect/platform/HttpApiClient"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Model from "../Model.ts"
import * as Error from "./Error.ts"

/**
 * The output schema endpoint (POST /output_schema).
 */
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema")`/output_schema`
  .addError(Error.DatasetStoreError)
  .addError(Error.SqlParseError)
  .addError(Error.PlanningError)
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
 * - SqlParseError: Failure in SQL parsing.
 * - PlanningError: Failure in planning the query.
 */
export type GetOutputSchemaError = Error.DatasetStoreError | Error.SqlParseError | Error.PlanningError

/**
 * The api definition for the registry api.
 */
export class Api extends HttpApi.make("registry").add(
  HttpApiGroup.make("registry", { topLevel: true }).add(getOutputSchema),
) {}

/**
 * Options for schema retrieval.
 */
export interface GetSchemaOptions {
  /**
   * Whether this is a sql dataset.
   *
   * @default true
   */
  isSqlDataset?: boolean
}

/**
 * The registry api service definition.
 */
export class Registry extends Context.Tag("Nozzle/Registry")<Registry, {
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
 * Creates a registry api service instance.
 *
 * @param url - The url of the registry api service.
 * @returns A registry api service instance.
 */
export const make = Effect.fn(function*(url: string) {
  const client = yield* HttpApiClient.make(Api, {
    baseUrl: url,
  })

  const getOutputSchema = Effect.fn("getOutputSchema")(function*(sql: string, options?: GetSchemaOptions) {
    const request = client.getOutputSchema({
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

  return { getOutputSchema }
})

/**
 * Creates a registry api service layer.
 *
 * @param url - The url of the registry api service.
 * @returns A layer for the registry api service.
 */
export const layer = (url: string) => make(url).pipe(Layer.effect(Registry), Layer.provide(FetchHttpClient.layer))
