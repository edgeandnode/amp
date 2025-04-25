import { createGrpcTransport } from "@connectrpc/connect-node"
import { Args, Command, Options } from "@effect/cli"
import { Config, Console, Effect, Match, Option, Schema, Stream } from "effect"
import * as Arrow from "../../Arrow.js"
import * as ArrowFlight from "../../ArrowFlight.js"

export const query = Command.make("query", {
  args: {
    query: Args.text({ name: "query" }).pipe(
      Args.withDescription("The SQL query"),
    ),
    limit: Options.integer("limit").pipe(
      Options.optional,
      Options.withDescription("The number of rows to return"),
    ),
    format: Options.choice("format", ["table", "json", "jsonl", "pretty"]).pipe(
      Options.withDefault("table"),
      Options.withDescription("The format to output the results in"),
    ),
    flight: Options.text("flight-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
      ),
      Options.withDescription("The Arrow Flight URL to use for the query"),
    ),
  },
}, ({ args }) => {
  const command = Effect.gen(function*() {
    const flight = yield* ArrowFlight.ArrowFlight
    const table = yield* flight.table(args.query).pipe(Effect.map((table) =>
      Option.match(args.limit, {
        onSome: (_) => table.slice(0, _),
        onNone: () => table,
      })
    ))

    const schema = Arrow.generateSchema(table.schema)
    yield* Match.value(args.format).pipe(
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
    ).pipe(Effect.catchTags({
      ParseError: (cause) => Effect.die(cause),
    }))
  })

  const layer = ArrowFlight.ArrowFlight.withTransport(createGrpcTransport({ baseUrl: args.flight }))
  return command.pipe(Effect.provide(layer))
}).pipe(
  Command.withDescription("Perform a Nozzle SQL query"),
)
