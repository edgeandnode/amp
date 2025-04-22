import { FetchHttpClient, HttpBody, HttpClient, HttpClientResponse, Ndjson, Template } from "@effect/platform"
import { Config, Data, Effect, Predicate, Schema, Stream } from "effect"

export class JsonLinesError extends Data.TaggedError("JsonLinesError")<{
  readonly cause: unknown
  readonly message?: string
}> {}

const ErrorResponse = Schema.Struct({ error: Schema.String })

export class JsonLines extends Effect.Service<JsonLines>()("Nozzle/JsonLines", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function*() {
    const url = yield* Config.string("NOZZLE_JSONL_URL").pipe(Effect.orDie)
    const client = yield* HttpClient.HttpClient

    const stream: {
      <A, I, R>(
        schema: Schema.Schema<A, I, R>
      ): (sql: TemplateStringsArray) => Stream.Stream<A, JsonLinesError, R>
      <A, I, R>(
        schema: Schema.Schema<A, I, R>
      ): (sql: string) => Stream.Stream<A, JsonLinesError, R>
    } = (schema) => (sql) => {
      return Stream.unwrap(Effect.gen(function*() {
        const query = typeof sql === "string" ? sql : yield* Template.make(sql)
        const response = yield* client.post(url, {
          body: HttpBody.text(query)
        }).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.catchTag("ResponseError", (cause) =>
            Effect.gen(function*() {
              if (cause.response.status !== 400) {
                return yield* cause
              }

              return yield* cause.response.text.pipe(
                Effect.map(JSON.parse),
                Effect.flatMap(Schema.decodeUnknown(ErrorResponse)),
                Effect.flatMap(({ error: message }) => new JsonLinesError({ cause, message }))
              )
            }))
        )

        return response.stream
      })).pipe(
        Stream.pipeThroughChannel(Ndjson.unpack({ ignoreEmptyLines: true })),
        Stream.mapEffect(Schema.decodeUnknown(schema)),
        Stream.mapError((cause) => Predicate.isTagged("JsonLinesError")(cause) ? cause : new JsonLinesError({ cause }))
      )
    }

    return { stream }
  })
}) {}
