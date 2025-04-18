import { FetchHttpClient, HttpBody, HttpClient, HttpClientError, Ndjson } from "@effect/platform";
import { Config, Data, Effect, ParseResult, Schema, Stream } from "effect";

export class JsonLinesError extends Data.TaggedError("JsonLinesError")<{
  readonly cause: HttpClientError.HttpClientError | Ndjson.NdjsonError | ParseResult.ParseError;
}> {}

export class JsonLines extends Effect.Service<JsonLines>()("Nozzle/Api/JsonLines", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const url = yield* Config.string("NOZZLE_JSONL_URL").pipe(Effect.orDie);
    const client = yield* HttpClient.HttpClient

    const stream = <A, I, R>(schema: Schema.Schema<A, I, R>, query: string) => Stream.unwrap(Effect.gen(function* () {
      const response = yield* client.post(url, {
        body: HttpBody.text(query),
      });

      return response.stream.pipe(
        Stream.pipeThroughChannel(Ndjson.unpack({ ignoreEmptyLines: true })),
        Stream.mapEffect(Schema.decodeUnknown(schema)),
      );
    })).pipe(Stream.mapError((cause) => new JsonLinesError({ cause })));

    return { stream };
  }),
}) {}
