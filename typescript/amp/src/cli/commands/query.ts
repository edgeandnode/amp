import type { Interceptor } from "@connectrpc/connect"
import { createGrpcTransport, type GrpcTransportOptions } from "@connectrpc/connect-node"
import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Chunk from "effect/Chunk"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ArrowFlight from "../../api/ArrowFlight.ts"
import * as Arrow from "../../Arrow.ts"
import * as Auth from "../../Auth.ts"
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

      // Decode every row from every record batch as it arrives.
      // This works for both streaming (infinite) and non-streaming (finite) queries.
      const rows = flight.query(args.query).pipe(
        Stream.flatMap((batch) => {
          const schema = Arrow.generateSchema(batch.schema)
          return Stream.fromIterable([...batch]).pipe(
            Stream.mapEffect(Schema.encodeUnknown(schema)),
          )
        }),
      )

      const limited = Option.match(args.limit, {
        onSome: (limit) => Stream.take(rows, limit),
        onNone: () => rows,
      })

      if (args.format === "jsonl") {
        yield* limited.pipe(
          Stream.map((_) => JSON.stringify(_)),
          Stream.runForEach(Console.log),
          Effect.catchTags({ ParseError: (cause) => Effect.die(cause) }),
        )
      } else if (args.format === "pretty") {
        yield* limited.pipe(
          Stream.runForEach(Console.log),
          Effect.catchTags({ ParseError: (cause) => Effect.die(cause) }),
        )
      } else {
        // table / json: collect rows then format. Stream.take above ensures this
        // terminates even for streaming queries when --limit is provided.
        const collected = yield* limited.pipe(
          Stream.runCollect,
          Effect.map(Chunk.toArray),
          Effect.catchTags({ ParseError: (cause) => Effect.die(cause) }),
        )
        if (args.format === "table") {
          yield* Console.table(collected)
        } else {
          yield* Console.log(JSON.stringify(collected, null, 2))
        }
      }

      // Force exit to close any open gRPC connections (streaming queries never
      // send EOS so the event loop would otherwise hang indefinitely).
      process.exit(0)
    }),
  ),
  Command.provide(({ args }) =>
    Layer.unwrapEffect(Effect.gen(function*() {
      const auth = yield* Auth.AuthService

      const maybeToken: Option.Option<string> = yield* args.bearerToken.pipe(
        Option.match({
          onSome: (token) => Effect.succeed(Option.some(token)),
          onNone: () =>
            auth.getCache().pipe(
              Effect.map(Option.map((authSchema) => `${authSchema.accessToken}`)),
            ),
        }),
      )

      const interceptors: Array<Interceptor> = maybeToken.pipe(
        Option.match({
          onSome: (token) => {
            const authInterceptor: Interceptor = (next) => (req) => {
              req.header.set("Authorization", `Bearer ${token}`)
              return next(req)
            }
            return [authInterceptor]
          },
          onNone: () => [],
        }),
      )

      const transportOptions: GrpcTransportOptions = {
        baseUrl: `${args.flightUrl}`,
        interceptors,
      }

      return ArrowFlight.layer(createGrpcTransport(transportOptions))
    })).pipe(Layer.provide(Auth.layer))
  ),
)
