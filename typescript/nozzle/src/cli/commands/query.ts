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
  const table = yield* flight.table(args.query);
  // Convert columnar record batches to rows for table output.
  const rows = table
    .toArray()
    .map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([k, v]) => [k, formatValue(v)]),
      ),
    );
  yield* Console.table(rows);
})).pipe(
  Command.withDescription("Run a Nozzle SQL query"),
  Command.provide(ArrowFlight.ArrowFlight.Default),
);

const formatValue = (value: unknown): unknown => {
  if (value instanceof Uint8Array && value.length <= 256) {
    return toHex(value);
  }
  return value;
};

const toHex = (bytes: Uint8Array): string =>
  "0x" +
  Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
