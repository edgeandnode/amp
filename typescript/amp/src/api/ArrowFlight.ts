import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, type Transport } from "@connectrpc/connect"
import * as Template from "@effect/platform/Template"
import type { RecordBatch } from "apache-arrow"
import { RecordBatchReader } from "apache-arrow"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Model from "../Model.ts"
import * as Flight from "../proto/Flight_pb.ts"
import * as FlightSql from "../proto/FlightSql_pb.ts"

export * as Flight from "../proto/Flight_pb.ts"
export * as FlightSql from "../proto/FlightSql_pb.ts"

/**
 * A record batch with metadata.
 */
export class ResponseBatch extends Data.TaggedClass("ResponseBatch")<{
  data: RecordBatch
  metadata: Model.RecordBatchMetadata
}> {}

/**
 * Error type for the Arrow Flight service.
 */
export class ArrowFlightError extends Data.TaggedError("ArrowFlightError")<{
  cause?: unknown
  message: string
}> {}

/**
 * Service definition for the Arrow Flight api.
 */
export class ArrowFlight extends Context.Tag("Amp/ArrowFlight")<ArrowFlight, {
  /**
   * The client for the Arrow Flight service.
   */
  readonly client: Client<typeof Flight.FlightService>

  /**
   * A stream of record batches from a sql query.
   *
   * @param sql - The sql query to execute.
   * @param watermark - Optional resume watermark for streaming.
   * @returns A stream of record batches.
   */
  readonly stream: {
    (sql: TemplateStringsArray): Stream.Stream<ResponseBatch, ArrowFlightError>
    (sql: string): Stream.Stream<ResponseBatch, ArrowFlightError>
  }
}>() {}

/**
 * Creates a new Arrow Flight service instance.
 *
 * @param transport - The transport to use for the Arrow Flight service instance.
 * @returns A new Arrow Flight service instance.
 */
export const make = (transport: Transport) => {
  const client = createClient(Flight.FlightService, transport)
  const stream: {
    (sql: TemplateStringsArray): Stream.Stream<ResponseBatch, ArrowFlightError>
    (sql: string): Stream.Stream<ResponseBatch, ArrowFlightError>
  } = (sql) =>
    Effect.gen(function*() {
      const query = typeof sql === "string" ? sql : yield* Template.make(sql)
      const cmd = create(FlightSql.CommandStatementQuerySchema, { query })
      const any = anyPack(FlightSql.CommandStatementQuerySchema, cmd)
      const descriptor = create(Flight.FlightDescriptorSchema, {
        type: Flight.FlightDescriptor_DescriptorType.CMD,
        cmd: toBinary(AnySchema, any),
      })

      const info = yield* Effect.tryPromise({
        try: (signal) => client.getFlightInfo(descriptor, { signal }),
        catch: (cause) => new ArrowFlightError({ cause, message: "Failed to get flight info" }),
      })

      const ticket = yield* Option.fromNullable(info.endpoint[0]?.ticket).pipe(
        Option.match({
          onNone: () => new ArrowFlightError({ message: "No flight ticket found" }),
          onSome: (ticket) => Effect.succeed(ticket),
        }),
      )

      const request = yield* Effect.async<AsyncIterable<Flight.FlightData>>((resume, signal) => {
        resume(Effect.sync(() => client.doGet(ticket, { signal })))
      })

      const meta: Array<Uint8Array> = []
      const ipc = Stream.fromAsyncIterable(request, (cause) =>
        new ArrowFlightError({ cause, message: "Failed to get flight data" })).pipe(
          Stream.map((data) => {
            meta.push(data.appMetadata)
            return flightDataToIpc(data)
          }),
          Stream.toReadableStream(),
        )

      const reader = yield* Effect.tryPromise({
        catch: (cause) =>
          new ArrowFlightError({ cause, message: "Failed to get flight data" }),
        try: () => RecordBatchReader.from(ipc),
      })

      return Stream.fromAsyncIterable(reader, (cause) =>
        new ArrowFlightError({ cause, message: "Failed to read record batches" })).pipe(Stream.map((data) => {
          const metadata = parseMetadata(meta.shift()!)
          return new ResponseBatch({ data, metadata })
        }))
    }).pipe(Stream.unwrap)

  return { client, stream }
}

/**
 * Creates a layer for the Arrow Flight service.
 *
 * @param transport - The transport to use for the Arrow Flight service.
 * @returns A layer for the Arrow Flight service.
 */
export const layer = (transport: Transport) => Layer.sync(ArrowFlight, () => make(transport))

/**
 * Converts a `FlightData` payload into Apache Arrow IPC format.
 */
const flightDataToIpc = (data: Flight.FlightData) => {
  // The data length needs to be padded to multiple of 8 bytes.
  const padding = data.dataBody.length % 8 === 0 ? 0 : 8 - (data.dataBody.length % 8)

  // Create buffer and write metadata prefix.
  const length = 8 + data.dataHeader.length + padding + data.dataBody.length
  const buf = new ArrayBuffer(length)
  const view = new DataView(buf)
  view.setUint32(0, 0xffffffff, true) // Continuation token
  view.setUint32(4, data.dataHeader.length, true) // Header length

  // Copy header and body into buffer.
  const bytes = new Uint8Array(buf)
  bytes.set(data.dataHeader, 8)
  bytes.set(data.dataBody, 8 + data.dataHeader.length + padding)

  return bytes
}

const textDecoder = new TextDecoder()
const decodeMetadata = Schema.decodeUnknownSync(Model.RecordBatchMetadata)

/**
 * Parses the response batch metadata from a Uint8Array.
 */
const parseMetadata = (bytes: Uint8Array): Model.RecordBatchMetadata => {
  const text = textDecoder.decode(bytes)
  return text === "" ? new Model.RecordBatchMetadata({ ranges: [] }) : decodeMetadata(JSON.parse(text))
}
