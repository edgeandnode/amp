import { createGrpcTransport } from "@connectrpc/connect-node"
import { expect, it } from "@effect/vitest"
import { type Table } from "apache-arrow"
import { Array, Config, Effect, Layer, Schema } from "effect"
import * as Arrow from "nozzl/Arrow"
import * as ArrowFlight from "nozzl/ArrowFlight"

const flight = Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(
  Effect.map((url) => createGrpcTransport({ baseUrl: url })),
  Effect.map((transport) => ArrowFlight.ArrowFlight.withTransport(transport)),
  Layer.unwrapEffect,
)

const rows = (table: Table) => [...table].map((row) => Object.fromEntries(Object.entries(row)))

it.layer(flight)((it) => {
  it.effect("should respond to a sql query", () =>
    Effect.gen(function*() {
      const flight = yield* ArrowFlight.ArrowFlight
      const table = yield* flight.table`
        SELECT * FROM VALUES (1, 2, 3, 4), (5, 6, 7, 8) AS TableLiteral(a, b, c, d)
      `

      expect(rows(table)).toStrictEqual([
        { a: 1n, b: 2n, c: 3n, d: 4n },
        { a: 5n, b: 6n, c: 7n, d: 8n },
      ])
    }))

  it.effect("should handle all datafusion data types", () =>
    Effect.gen(function*() {
      const flight = yield* ArrowFlight.ArrowFlight

      // TODO: Cover all data types.
      const table = yield* flight.table`SELECT
        arrow_cast(null, 'Null') AS null_col,
        arrow_cast(true, 'Boolean') AS boolean_col,
        arrow_cast(1, 'Int8') AS int8_col,
        arrow_cast(1, 'Int16') AS int16_col,
        arrow_cast(1, 'Int32') AS int32_col,
        arrow_cast(1, 'Int64') AS int64_col,
        arrow_cast(1, 'UInt8') AS uint8_col,
        arrow_cast(1, 'UInt16') AS uint16_col,
        arrow_cast(1, 'UInt32') AS uint32_col,
        arrow_cast(1, 'UInt64') AS uint64_col,
        arrow_cast(1.2, 'Float16') AS float16_col,
        arrow_cast(1.3, 'Float32') AS float32_col,
        arrow_cast(1.4, 'Float64') AS float64_col,
        arrow_cast('Hello', 'Utf8') AS utf8_col,
        arrow_cast('Hello', 'LargeUtf8') AS large_utf8_col,
        arrow_cast('Hello', 'Binary') AS binary_col,
        arrow_cast(now(), 'Timestamp(Second, None)') AS timestamp_col,
        arrow_cast(now(), 'Timestamp(Millisecond, None)') AS timestamp_millis_col,
        arrow_cast(now(), 'Timestamp(Microsecond, None)') AS timestamp_micros_col,
        arrow_cast(now(), 'Timestamp(Nanosecond, None)') AS timestamp_nanos_col,
        arrow_cast(now(), 'Time32(Second)') AS time32_seconds_col,
        arrow_cast(now(), 'Time64(Microsecond)') AS time64_micros_col
      `

      const data = yield* Array.head([...table])
      const schema = Arrow.generateSchema(table.schema)
      expect(schema.ast).toEqual(
        Schema.Struct({
          null_col: Schema.Null,
          boolean_col: Schema.Boolean,
          int8_col: Schema.Int,
          int16_col: Schema.Int,
          int32_col: Schema.Int,
          int64_col: Schema.BigInt,
          uint8_col: Schema.NonNegativeInt,
          uint16_col: Schema.NonNegativeInt,
          uint32_col: Schema.NonNegativeInt,
          uint64_col: Schema.NonNegativeBigInt,
          float16_col: Schema.Number,
          float32_col: Schema.Number,
          float64_col: Schema.Number,
          utf8_col: Schema.String,
          large_utf8_col: Schema.String,
          binary_col: Schema.Uint8ArrayFromHex,
          timestamp_col: Schema.Number,
          timestamp_millis_col: Schema.Number,
          timestamp_micros_col: Schema.Number,
          timestamp_nanos_col: Schema.Number,
          time32_seconds_col: Schema.Number,
          time64_micros_col: Schema.BigInt,
        }).ast,
      )

      yield* Schema.validate(schema)(data)
    }))
})
