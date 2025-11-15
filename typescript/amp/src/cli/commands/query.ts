import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import { Table } from "apache-arrow"
import * as Chunk from "effect/Chunk"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ArrowFlight from "../../api/ArrowFlight.ts"
import * as Arrow from "../../Arrow.ts"
import { flightUrl } from "../common.ts"

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
    bearerToken: Options.text("bearer-token").pipe(
      Options.withDescription("Bearer auth token"),
      Options.optional,
    ),
    flightUrl,
  },
}).pipe(
  Command.withDescription("Perform a Amp SQL query"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const flight = yield* ArrowFlight.ArrowFlight
      const table = yield* flight.query(args.query).pipe(
        Stream.runCollect,
        Effect.map((_) => Chunk.toArray(_)),
        Effect.map((array) =>
          Option.match(args.limit, {
            onSome: (limit) => new Table(array.slice(0, limit).map((response) => response.data)),
            onNone: () => new Table(array.map((response) => response.data)),
          })
        ),
      )

      const schema = Arrow.generateSchema(table.schema)
      const effect = Match.value(args.format).pipe(
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

      yield* effect.pipe(Effect.catchTags({
        ParseError: (cause) => Effect.die(cause),
      }))
    }),
  ),
  Command.provide(({ args }) => {
    const transportOptions: Parameters<typeof createGrpcTransport>[0] = {
      baseUrl: `${args.flightUrl}`,
    }

    if (Option.isSome(args.bearerToken)) {
      const token = args.bearerToken.value
      transportOptions.interceptors = [
        (next) => (req) => {
          req.header.set("Authorization", `Bearer ${token}`)
          return next(req)
        },
      ]
    }

    return ArrowFlight.layer(createGrpcTransport(transportOptions))
  }),
)
