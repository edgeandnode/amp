import type { Interceptor, Transport } from "@connectrpc/connect"
import { Table } from "apache-arrow"
import * as Effect from "effect/Effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ArrowFlight from "./api/ArrowFlight.ts"
import * as Arrow from "./Arrow.ts"
import type * as Model from "./Model.ts"

export * as StudioModel from "./studio/Model.ts"

/**
 * Admin client for managing service
 */
export * as Admin from "./api/Admin.ts"

/**
 * Arrow Flight protocol implementation
 */
export * as ArrowFlight from "./api/ArrowFlight.ts"

/**
 * Error types and utilities
 */
export * as ApiError from "./api/Error.ts"

/**
 * Apache Arrow utilities and helpers
 */
export * as Arrow from "./Arrow.ts"

/**
 * Data models and type definitions
 */
export * as Model from "./Model.ts"

// TODO: Temporary re-export for backwards compatibility
export * from "./config.ts"

/**
 * Client interface for querying blockchain data
 *
 * Returns results as Apache Arrow RecordBatch streams.
 */
export interface AmpClient {
  /**
   * Execute a streaming query with optional resumption from specific block ranges
   *
   * @param query - SQL query string to execute
   * @param resume - Optional array of block ranges to resume from
   * @returns Async iterable of decoded record batches
   *
   * @example
   * ```typescript
   * const client = createClient(transport)
   * for await (const batch of client.stream("SELECT * FROM eth_rpc.logs")) {
   *   console.log(batch.numRows)
   * }
   * ```
   */
  stream: <T = any>(query: string, resume?: ReadonlyArray<Model.BlockRange>) => AsyncIterable<Array<T>>

  /**
   * Execute a one-time query
   *
   * @param query - SQL query string to execute
   * @returns Async iterable of decoded record batches
   *
   * @example
   * ```typescript
   * const client = createClient(transport)
   * for await (const batch of client.query("SELECT * FROM eth_rpc.blocks LIMIT 10")) {
   *   console.log(batch.numRows)
   * }
   * ```
   */
  query: <T = any>(query: string) => AsyncIterable<Array<T>>
}

/**
 * Create a query client
 *
 * Creates a client that connects to Amp using the provided transport.
 *
 * The client uses the Arrow Flight protocol for efficient data transfer and supports
 * both streaming and batch queries.
 *
 * @param transport - Transport for communicating with the Amp server
 * @returns Streaming and batch query client instance
 *
 * @example
 * ```typescript
 * import { createClient } from "@edgeandnode/amp"
 * import { createConnectTransport } from "@connectrpc/connect-node"
 *
 * const transport = createConnectTransport({
 *   baseUrl: "http://localhost:1602",
 *   httpVersion: "2",
 *   interceptors: [createAuthInterceptor("your-api-token")]
 * })
 *
 * const client = createClient(transport)
 * for await (const batch of client.stream("SELECT * FROM eth_rpc.logs")) {
 *   console.log(`Received ${batch.data.numRows} rows`)
 * }
 * ```
 */
export const createClient = (transport: Transport) => {
  const layer = ArrowFlight.layer(transport)
  const runtime = ManagedRuntime.make(layer)

  return {
    stream: (query: string, resume?: ReadonlyArray<Model.BlockRange>) =>
      Effect.gen(function*() {
        const flight = yield* ArrowFlight.ArrowFlight
        return flight.stream(query, resume).pipe(
          Stream.mapEffect(Effect.fn(function*(batch) {
            const schema = Schema.Array(Arrow.generateSchema(batch.data.schema))
            const table = new Table(batch.data)
            const data = yield* Schema.encode(schema)([...table])
            return data
          })),
          Stream.toAsyncIterable,
        )
      }).pipe(runtime.runSync),
    query: (query: string) =>
      Effect.gen(function*() {
        const flight = yield* ArrowFlight.ArrowFlight
        return flight.query(query).pipe(
          Stream.mapEffect(Effect.fn(function*(batch) {
            const schema = Schema.Array(Arrow.generateSchema(batch.schema))
            const table = new Table(batch)
            const data = yield* Schema.encode(schema)([...table])
            return data
          })),
          Stream.toAsyncIterable,
        )
      }).pipe(runtime.runSync),
  }
}

/**
 * Create an authentication interceptor
 *
 * Creates an interceptor that adds a Bearer token to all outgoing requests.
 *
 * Use this interceptor when connecting to an authenticated Amp server.
 *
 * @param token - Token to use for authentication
 * @returns Interceptor that adds Authorization header
 *
 * @example
 * ```typescript
 * import { createClient, createAuthInterceptor } from "@edgeandnode/amp"
 * import { createConnectTransport } from "@connectrpc/connect-node"
 *
 * const transport = createConnectTransport({
 *   baseUrl: "https://amp.example.com",
 *   httpVersion: "2",
 *   interceptors: [createAuthInterceptor("your-api-token")]
 * })
 *
 * const client = createClient(transport)
 * for await (const batch of client.query("SELECT * FROM eth_rpc.blocks LIMIT 10")) {
 *   console.log(batch)
 * }
 * ```
 */
export const createAuthInterceptor = (token: string): Interceptor => {
  return (next) => (request) => {
    request.header.set("Authorization", `Bearer ${token}`)
    return next(request)
  }
}
