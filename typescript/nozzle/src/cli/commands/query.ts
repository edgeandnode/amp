import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ArrowFlight from "../../api/ArrowFlight.ts"
import * as Arrow from "../../Arrow.ts"

export const query = Command.make("query", {
  args: {
    query: Args.text({ name: "query" }).pipe(
      Args.withDescription("The SQL query"),
    ),
    limit: Options.integer("limit").pipe(
      Options.withDescription("The number of rows to return"),
      Options.optional,
    ),
    format: Options.choice("format", ["table", "json", "jsonl", "pretty"]).pipe(
      Options.withDescription("The format to output the results in"),
      Options.withDefault("table"),
    ),
    flight: Options.text("flight-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
      ),
      Options.withDescription("The Arrow Flight URL to use for the query"),
      Options.withSchema(Schema.URL),
    ),
  },
}).pipe(
  Command.withDescription("Perform a Nozzle SQL query"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const flight = yield* ArrowFlight.ArrowFlight
      const table = yield* flight
        .table(args.query)
        .pipe(
          Effect.map((table) => Option.match(args.limit, { onSome: (_) => table.slice(0, _), onNone: () => table })),
        )

      const schema = Arrow.generateSchema(table.schema)
      yield* Match.value(args.format)
        .pipe(
          Match.when("table", () =>
            Effect.succeed([...table]).pipe(
              Effect.flatMap(Schema.encodeUnknown(Schema.Array(schema))),
              Effect.flatMap(Console.table),
            )),
          Match.when("json", () =>
            Effect.succeed([...table]).pipe(
              Effect.flatMap(Schema.encodeUnknown(Schema.Array(schema))),
              Effect.map((_) => JSON.stringify(_, null, 2)),
              Effect.flatMap(Console.log),
            )),
          Match.when("jsonl", () =>
            Stream.fromIterable([...table]).pipe(
              Stream.mapEffect(Schema.encodeUnknown(schema)),
              Stream.map((_) => JSON.stringify(_)),
              Stream.runForEach(Console.log),
            )),
          Match.when("pretty", () =>
            Stream.fromIterable([...table]).pipe(
              Stream.mapEffect(Schema.encodeUnknown(schema)),
              Stream.runForEach(Console.log),
            )),
          Match.exhaustive,
        )
        .pipe(Effect.catchTags({ ParseError: (cause) => Effect.die(cause) }))
    }),
  ),
  Command.provide(({ args }) => ArrowFlight.layer(createGrpcTransport({ baseUrl: `${args.flight}` }))),
)
