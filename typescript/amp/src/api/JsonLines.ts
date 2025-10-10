import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
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

export const JsonLinesErrorUnion = Schema.Union(Error.DatasetStoreError)

/**
 * Error type for the json lines api service.
 */
export type JsonLinesError = Error.DatasetStoreError

/**
 * Service definition for the jsonl api.
 */
export class JsonLines extends Context.Tag("Amp/JsonLines")<JsonLines, {
  /**
   * Creates a stream of results from a sql query.
   *
   * @param schema - The schema of the results.
   * @param sql - The sql query to execute.
   * @returns A stream of results.
   */
  readonly stream: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: TemplateStringsArray) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: string) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
  }

  /**
   * Queries the json lines api and returns the results.
   *
   * @param schema - The schema of the results.
   * @param sql - The sql query to execute.
   * @returns The results.
   */
  readonly query: {
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: TemplateStringsArray) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: string) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
  }
}>() {}

/**
 * Creates a json lines api service.
 *
 * @param url - The url for the json lines api.
 * @returns The json lines api service.
 */
export const make = Effect.fn(function*(url: string) {
  const client = yield* HttpClient.HttpClient.pipe(
    Effect.map(HttpClient.mapRequest(HttpClientRequest.setHeader("Accept-Encoding", "deflate"))),
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
    ): (sql: TemplateStringsArray) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: string) => Stream.Stream<A, HttpClientError.HttpClientError | JsonLinesError, R>
  } = (schema) => (sql) => {
    return Stream.unwrap(
      Effect.gen(function*() {
        const query = typeof sql === "string" ? sql : yield* Template.make(sql)
        const response = yield* client.post(url, { body: HttpBody.text(query) })

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
    ): (sql: TemplateStringsArray) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
    <A, I, R>(
      schema: Schema.Schema<A, I, R>,
    ): (sql: string) => Effect.Effect<Array<A>, HttpClientError.HttpClientError | JsonLinesError, R>
  } = (schema) => (sql) =>
    stream(schema)(sql as any).pipe(
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
export const layer = (url: string) => make(url).pipe(Layer.effect(JsonLines), Layer.provide(FetchHttpClient.layer))
