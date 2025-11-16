import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, type Transport } from "@connectrpc/connect"
import * as Template from "@effect/platform/Template"
import type { RecordBatch } from "apache-arrow"
import { RecordBatchReader, tableFromIPC } from "apache-arrow"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Model from "../Model.ts"
import * as Flight from "../proto/Flight_pb.ts"
import * as FlightSql from "../proto/FlightSql_pb.ts"

export * as Flight from "../proto/Flight_pb.ts"
export * as FlightSql from "../proto/FlightSql_pb.ts"

export class ArrowFlightError extends Data.TaggedError("ArrowFlightError")<{
  method: string
  cause: unknown
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
   * A stream of protocol messages from a sql query.
   *
   * Implements stateless reorg detection by comparing incoming block ranges.
   *
   * @param sql - The sql query to execute.
   * @param resume - Optional block ranges to resume from.
   * @returns A stream of protocol messages.
   */
  readonly stream: {
    (
      sql: TemplateStringsArray,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<Model.ResponseBatch, ArrowFlightError>
    (
      sql: string,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<Model.ResponseBatch, ArrowFlightError>
  }

  /**
   * A batch query from a sql query.
   *
   * @param sql - The sql query to execute.
   * @param resume - Optional block ranges to resume from.
   * @returns A stream of record batches.
   */
  readonly query: {
    (
      sql: TemplateStringsArray,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<RecordBatch, ArrowFlightError>
    (
      sql: string,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<RecordBatch, ArrowFlightError>
  }
}>() {}

const createResponseStream: (
  client: Client<typeof Flight.FlightService>,
  sql: string | TemplateStringsArray,
  stream?: boolean,
  resume?: ReadonlyArray<Model.BlockRange>,
) => Effect.Effect<Stream.Stream<[RecordBatch, Uint8Array], ArrowFlightError>, ArrowFlightError> = Effect.fn(
  function*(client, sql, stream, resume) {
    const query = typeof sql === "string" ? sql : yield* Template.make(sql)
    const cmd = create(FlightSql.CommandStatementQuerySchema, { query })
    const any = anyPack(FlightSql.CommandStatementQuerySchema, cmd)
    const descriptor = create(Flight.FlightDescriptorSchema, {
      type: Flight.FlightDescriptor_DescriptorType.CMD,
      cmd: toBinary(AnySchema, any),
    })

    const [ticket, schema] = yield* Effect.tryPromise({
      try: async (signal) => {
        const headers = new Headers({
          ...(stream === true ? { "amp-stream": "true" } : {}),
          ...(resume !== undefined ? { "amp-resume": blockRangesToResumeWatermark(resume) } : {}),
        })

        const info = await client.getFlightInfo(descriptor, { signal, headers })
        const ticket = info.endpoint[0]?.ticket
        if (ticket === undefined) {
          throw new Error("Missing flight ticket in response")
        }

        const table = tableFromIPC(info.schema)
        return [ticket, table.schema] as const
      },
      catch: (cause) => new ArrowFlightError({ cause, method: "getFlightInfo" }),
    })

    const request = yield* Effect.async<AsyncIterable<Flight.FlightData>>((resume, signal) => {
      resume(Effect.sync(() => client.doGet(ticket, { signal })))
    })

    let meta: Uint8Array
    const ipc = Stream.fromAsyncIterable(request, (cause) => new ArrowFlightError({ cause, method: "doGet" })).pipe(
      Stream.map((data) => {
        // NOTE: This is a hack to forward the app metadata through the stream.
        meta = data.appMetadata
        return flightDataToIpc(data)
      }),
      Stream.toReadableStream(),
    )

    const reader = yield* Effect.tryPromise({
      catch: (cause) => new ArrowFlightError({ cause, method: "doGet" }),
      try: () => RecordBatchReader.from(ipc),
    })

    return Stream.fromAsyncIterable(reader, (cause) => new ArrowFlightError({ cause, method: "doGet" })).pipe(
      Stream.map((data) => {
        // NOTE: This is a hack but our schemas are stable and the `RecordBatch` constructor creates new schema instances for no reason.
        ;(data as any).schema = schema
        return [data, meta] as const
      }),
    )
  },
)

/**
 * Creates a new Arrow Flight service instance.
 *
 * @param transport - The transport to use for the Arrow Flight service instance.
 * @returns A new Arrow Flight service instance.
 */
export const make = (transport: Transport) => {
  const decode = Schema.decodeSync(Model.RecordBatchMetadataFromFlight)
  const client = createClient(Flight.FlightService, transport)
  const stream: {
    (
      sql: TemplateStringsArray,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<Model.ResponseBatch, ArrowFlightError>
    (
      sql: string,
      resume?: ReadonlyArray<Model.BlockRange>,
    ): Stream.Stream<Model.ResponseBatch, ArrowFlightError>
  } = (sql, resume) =>
    createResponseStream(client, sql, true, resume).pipe(
      Stream.unwrap,
      Stream.map(([data, metadata]) => new Model.ResponseBatch({ data, metadata: decode(metadata) })),
    )

  const query = (
    sql: TemplateStringsArray | string,
  ): Stream.Stream<RecordBatch, ArrowFlightError> => {
    const responses = createResponseStream(client, sql).pipe(
      Stream.unwrap,
      Stream.map(([data]) => data),
    )

    return responses
  }

  return { client, stream, query }
}

/**
 * Creates a layer for the Arrow Flight service.
 *
 * @param transport - The transport to use for the Arrow Flight service.
 * @returns A layer for the Arrow Flight service.
 */
export const layer = (transport: Transport) => Layer.sync(ArrowFlight, () => make(transport))

/**
 * Converts a list of block ranges into a resume watermark string.
 *
 * @param ranges - The block ranges to convert.
 * @returns A resume watermark string.
 */
const blockRangesToResumeWatermark = (ranges: ReadonlyArray<Model.BlockRange>): string => {
  const watermarks: Record<string, { number: number; hash: string }> = {}
  for (const range of ranges) {
    watermarks[range.network] = {
      number: range.numbers.end,
      hash: range.hash,
    }
  }

  return JSON.stringify(watermarks)
}

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
