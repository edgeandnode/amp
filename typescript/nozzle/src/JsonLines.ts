import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest, Ndjson, Template } from "@effect/platform"
import { Config, Data, Effect, Layer, Predicate, Schema, Stream } from "effect"

export class JsonLinesError extends Data.TaggedError("JsonLinesError")<{
  readonly cause: unknown
  readonly message: string
}> {}

export class JsonLinesErrorResponse extends Schema.Class<JsonLinesErrorResponse>("JsonLinesErrorResponse")({
  error: Schema.String,
}) {
  readonly _tag = "JsonLinesErrorResponse" as const
}

const make = (url: string) =>
  Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient.pipe(
      Effect.map(HttpClient.filterStatusOk),
      Effect.map(HttpClient.transformResponse(Effect.catchTag("ResponseError", (cause) =>
        cause.response.text.pipe(
          Effect.tryMap({
            try: (_) => JSON.parse(_),
            catch: () => new JsonLinesError({ cause, message: "Malformed response" }),
          }),
          Effect.flatMap(Schema.decodeUnknown(JsonLinesErrorResponse)),
          Effect.flatMap(({ error }) => new JsonLinesError({ cause, message: error })),
        )))),
      Effect.map(
        HttpClient.mapRequest(HttpClientRequest.setHeader("Accept-Encoding", "deflate")),
      ),
    )

    const stream: {
      <A, I, R>(
        schema: Schema.Schema<A, I, R>,
      ): (sql: TemplateStringsArray) => Stream.Stream<A, JsonLinesError, R>
      <A, I, R>(
        schema: Schema.Schema<A, I, R>,
      ): (sql: string) => Stream.Stream<A, JsonLinesError, R>
    } = (schema) => (sql) => {
      return Stream.unwrap(Effect.gen(function*() {
        const query = typeof sql === "string" ? sql : yield* Template.make(sql)
        const response = yield* client.post(url, {
          body: HttpBody.text(query),
        })

        return response.stream
      })).pipe(
        Stream.pipeThroughChannel(Ndjson.unpack({ ignoreEmptyLines: true })),
        Stream.mapEffect(Schema.decodeUnknown(schema)),
        Stream.mapError((cause) =>
          Predicate.isTagged("JsonLinesError")(cause)
            ? cause
            : new JsonLinesError({ cause, message: "Malformed response" })
        ),
      )
    }

    return { stream }
  })

export class JsonLines extends Effect.Service<JsonLines>()("Nozzle/JsonLines", {
  dependencies: [FetchHttpClient.layer],
  effect: Config.string("NOZZLE_JSONL_URL").pipe(Effect.flatMap(make), Effect.orDie),
}) {}

export const layerJsonLines = (url: string) =>
  make(url).pipe(Effect.map(JsonLines.make), Layer.effect(JsonLines), Layer.provide(FetchHttpClient.layer))
