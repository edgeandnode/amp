import { Args, Command } from "@effect/cli";
import { Console, Effect } from "effect";
import * as ArrowFlight from "../../ArrowFlight.js";

export const query = Command.make("query", {
  args: {
    query: Args.text({ name: "query" }).pipe(
      Args.withDescription("The SQL query string"),
    ),
    limit: Args.integer({ name: "limit" }).pipe(
      Args.withDefault(10),
      Args.withDescription("The number of rows to return"),
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const flight = yield* ArrowFlight.ArrowFlight;
  const table = yield* flight.table(args.query)

  yield* Console.log(table.toString())
})).pipe(
  Command.withDescription("Run a Nozzle SQL query"),
  Command.provide(ArrowFlight.ArrowFlight.Default),
);
