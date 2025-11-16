import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import type * as Headers from "@effect/platform/Headers"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as Ndjson from "@effect/platform/Ndjson"
import * as Template from "@effect/platform/Template"
import * as Chunk from "effect/Chunk"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Error from "./Error.ts"

export const JsonLinesErrorUnion = Schema.Union(
  Error.DatasetStoreError,
  Error.PlanningError,
  Error.CatalogForSqlError,
)

/**
 * Error type for the json lines api service.
 */
export type JsonLinesError = Error.DatasetStoreError | Error.PlanningError | Error.CatalogForSqlError

/**
 * Service definition for the jsonl api.
 */
export class JsonLines extends Context.Tag("Amp/JsonLines")<JsonLines, {
  /**
   * Creates a stream of results from a sql query.
   *
   * @param schema - The schema of the results.
   * @param sql - The sql query to execute.
   * @param headers - Additional headers to append to the client request
   * @returns A stream of results.
   */
  readonly stream: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: TemplateStringsArray,
      headers?: Headers.Input | undefined,
    ) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: string,
      headers?: Headers.Input | undefined,
    ) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
  }

  /**
   * Queries the json lines api and returns the results.
   *
   * @param schema - The schema of the results.
   * @param sql - The sql query to execute.
   * @param headers - Additional headers to append to the client request
   * @returns The results.
   */
  readonly query: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: TemplateStringsArray,
      headers?: Headers.Input | undefined,
    ) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: string,
      headers?: Headers.Input | undefined,
    ) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
  }
}>() {}

export interface JsonLinesOptions {
  bearerToken?: string | undefined
}

/**
 * Creates a json lines api service.
 *
 * @param url - The url for the json lines api.
 * @returns The json lines api service.
 */
export const make = Effect.fn(function*(url: string, options?: JsonLinesOptions) {
  const client = yield* HttpClient.HttpClient.pipe(
    Effect.map(HttpClient.mapRequest(HttpClientRequest.setHeaders({
      "Accept-Encoding": "deflate",
      "Accept": "application/jsonl",
      ...(options?.bearerToken !== undefined ? { "Authorization": `Bearer ${options.bearerToken}` } : undefined),
    }))),
    Effect.map(HttpClient.transformResponse(
      Effect.flatMap((response) => {
        if (response.status >= 200 && response.status < 300) {
          return Effect.succeed(response)
        }

        return response.json.pipe(
          Effect.flatMap(Schema.decodeUnknown(JsonLinesErrorUnion)),
          Effect.orDie,
          Effect.flatMap(Effect.fail),
        )
      }),
    )),
  )

  const stream: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: TemplateStringsArray,
      headers?: Headers.Input | undefined,
    ) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: string,
      headers?: Headers.Input | undefined,
    ) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
  } = (schema) => (sql, headers) => {
    return Stream.unwrap(
      Effect.gen(function*() {
        const query = typeof sql === "string" ? sql : yield* Template.make(sql)
        const response = yield* client.post(url, { body: HttpBody.text(query), headers })

        return response.stream
      }),
    ).pipe(
      Stream.pipeThroughChannel(Ndjson.unpack({ ignoreEmptyLines: true })),
      Stream.mapEffect(Schema.decodeUnknown(schema)),
      Stream.catchTags({
        ParseError: Effect.die,
        NdjsonError: Effect.die,
      }),
    )
  }

  const query: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: TemplateStringsArray,
      headers?: Headers.Input | undefined,
    ) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (
      sql: string,
      headers?: Headers.Input | undefined,
    ) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
  } = (schema) => (sql, headers) =>
    stream(schema)(sql as any, headers).pipe(
      Stream.runCollect,
      Effect.map((_) => Chunk.toArray(_)),
    )

  return { stream, query }
})

/**
 * Creates a json lines api service layer.
 *
 * @param url - The url for the json lines api.
 * @returns The json lines api service layer.
 */
export const layer = (url: string, options?: JsonLinesOptions) =>
  make(url, options).pipe(Layer.effect(JsonLines), Layer.provide(FetchHttpClient.layer))
