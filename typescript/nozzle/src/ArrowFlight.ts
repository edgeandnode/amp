import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import type { Transport } from "@connectrpc/connect"
import { createClient } from "@connectrpc/connect"
import { Template } from "@effect/platform"
import type { RecordBatch } from "apache-arrow"
import { RecordBatchReader, Table } from "apache-arrow"
import { Chunk, Context, Data, Effect, Layer, Option, Stream } from "effect"
import * as Flight from "./proto/Flight_pb.js"
import * as FlightSql from "./proto/FlightSql_pb.js"

export * as Flight from "./proto/Flight_pb.js"
export * as FlightSql from "./proto/FlightSql_pb.js"

export class ArrowFlightError extends Data.TaggedError("ArrowFlightError")<{
  cause?: unknown
  message: string
}> {}

export class ArrowFlight extends Context.Tag("Nozzle/ArrowFlight")<ArrowFlight, ReturnType<typeof make>>() {
  static withTransport(transport: Transport) {
    return Layer.sync(ArrowFlight, () => make(transport))
  }
}

const make = (transport: Transport) => {
  const client = createClient(Flight.FlightService, transport)
  const stream: {
    (sql: TemplateStringsArray): Stream.Stream<RecordBatch, ArrowFlightError>
    (sql: string): Stream.Stream<RecordBatch, ArrowFlightError>
  } = (sql) =>
    Effect.gen(function*() {
      const query = typeof sql === "string" ? sql : yield* Template.make(sql)
      const cmd = create(FlightSql.CommandStatementQuerySchema, { query })
      // TODO: Why is it necessary to pack the command into an Any?
      const any = anyPack(FlightSql.CommandStatementQuerySchema, cmd)
      const descriptor = create(Flight.FlightDescriptorSchema, {
        type: Flight.FlightDescriptor_DescriptorType.CMD,
        cmd: toBinary(AnySchema, any),
      })

      const info = yield* Effect.tryPromise({
        try: (_) => client.getFlightInfo(descriptor, { signal: _ }),
        catch: (cause) => new ArrowFlightError({ cause, message: "Failed to get flight info" }),
      })

      const ticket = yield* Option.fromNullable(info.endpoint[0]?.ticket).pipe(Option.match({
        onNone: () => new ArrowFlightError({ message: "No flight ticket found" }),
        onSome: (ticket) => Effect.succeed(ticket),
      }))

      const request = yield* Effect.async<AsyncIterable<Flight.FlightData>>((resume, signal) => {
        resume(Effect.sync(() => client.doGet(ticket, { signal })))
      })

      const reader = yield* Effect.tryPromise({
        catch: (cause) => new ArrowFlightError({ cause, message: "Failed to get flight data" }),
        try: () =>
          RecordBatchReader.from({
            async *[Symbol.asyncIterator]() {
              for await (const data of request) {
                // The data length needs to be padded to multiple of 8 bytes.
                const padding = data.dataBody.length % 8 === 0 ? 0 : 8 - (data.dataBody.length % 8)

                // Create buffer and write metadata prefix.
                const length = 8 + data.dataHeader.length + padding + data.dataBody.length
                const buf = new ArrayBuffer(length)
                const view = new DataView(buf)
                view.setUint32(0, 0xFFFFFFFF, true) // Continuation token
                view.setUint32(4, data.dataHeader.length, true) // Header length

                // Copy header and body into buffer.
                const bytes = new Uint8Array(buf)
                bytes.set(data.dataHeader, 8)
                bytes.set(data.dataBody, 8 + data.dataHeader.length + padding)

                yield buf
              }
            },
          } as AsyncIterable<ArrayBuffer>),
      })

      return Stream.fromAsyncIterable(reader, (cause) =>
        new ArrowFlightError({ cause, message: "Failed to read record batches" }))
    }).pipe(Stream.unwrap)

  const table: {
    (sql: TemplateStringsArray): Effect.Effect<Table, ArrowFlightError>
    (sql: string): Effect.Effect<Table, ArrowFlightError>
  } = (sql: any) =>
    Stream.runCollect(stream(sql)).pipe(
      Effect.map((batches) => new Table(Chunk.toArray(batches))),
    ) as any

  return { client, stream, table }
}
