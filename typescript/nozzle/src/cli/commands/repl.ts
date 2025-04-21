import { createGrpcTransport } from "@connectrpc/connect-node"
import { Command } from "@effect/cli"
import { Config, Console, Effect, Schema } from "effect"
import * as rep from "node:repl"
import * as ArrowFlight from "../../ArrowFlight.js"

export const repl = Command.make("repl", {}, () =>
  Effect.gen(function*() {
    const flight = yield* ArrowFlight.ArrowFlight
    const server = rep.start("> ")
    server.defineCommand("query", {
      help: "Perform a Nozzle SQL query",
      action: (query) => {
        const program = Effect.gen(function*() {
          const table = yield* flight.table(query)
          const schema = ArrowFlight.generateSchema(table.schema)
          yield* Effect.succeed([...table]).pipe(
            Effect.flatMap(Schema.encodeUnknown(Schema.Array(schema))),
            Effect.flatMap(Console.table)
          )
        })

        return Effect.runPromise(program.pipe(
          Effect.catchTag("ArrowFlightError", (error) => Console.error(error.message))
        ))
      }
    })

    yield* Effect.never
  })).pipe(
    Command.withDescription("Open a REPL for Nozzle SQL queries"),
    Command.provide(ArrowFlight.layerEffect(Effect.gen(function*() {
      const url = yield* Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Effect.orDie)
      return createGrpcTransport({ baseUrl: url })
    })))
  )
