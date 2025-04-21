import { createGrpcTransport } from "@connectrpc/connect-node"
import { expect, it } from "@effect/vitest"
import type { Table } from "apache-arrow"
import { Config, Effect } from "effect"
import * as ArrowFlight from "../src/ArrowFlight.js"

const transport = Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(
  Effect.map((url) => createGrpcTransport({ baseUrl: url }))
)

const rows = (table: Table) => [...table].map((row) => Object.fromEntries(Object.entries(row)))

it.layer(ArrowFlight.layerEffect(transport))((it) => {
  it.effect("should respond to a sql query", () =>
    Effect.gen(function*() {
      const flight = yield* ArrowFlight.ArrowFlight
      const table = yield* flight.table`
        SELECT * FROM VALUES (1, 2, 3, 4), (5, 6, 7, 8) AS TableLiteral(a, b, c, d)
      `

      expect(rows(table)).toStrictEqual([
        { a: 1n, b: 2n, c: 3n, d: 4n },
        { a: 5n, b: 6n, c: 7n, d: 8n }
      ])
    }))
})
