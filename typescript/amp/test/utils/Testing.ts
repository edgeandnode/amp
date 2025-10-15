import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Admin from "@edgeandnode/amp/api/Admin"
import * as ArrowFlight from "@edgeandnode/amp/api/ArrowFlight"
import * as JsonLines from "@edgeandnode/amp/api/JsonLines"
import * as EvmRpc from "@edgeandnode/amp/evm/EvmRpc"
import type * as Model from "@edgeandnode/amp/Model"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Vitest from "@effect/vitest"
import * as Config from "effect/Config"
import * as Data from "effect/Data"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Fixtures from "./Fixtures.ts"

class JobIncompleteError extends Data.TaggedError("JobIncompleteError")<{
  readonly cause?: unknown
  readonly job: Model.JobInfo
}> {}

/**
 * Waits for a job to complete by polling its status.
 *
 * @param id The job ID.
 * @param attempts Maximum number of polling attempts (default: 30)
 * @param interval Polling interval (e.g. "200 millis")
 * @returns An effect that yields the completed JobInfo or fails with a `JobIncompleteError` if the job errors or times out
 */
export const waitForJobCompletion = Effect.fn("waitForJobCompletion")(function*(
  id: number,
  options?: {
    attempts?: number
    interval?: Duration.DurationInput
  },
) {
  const {
    attempts = 30,
    interval = 200,
  } = options ?? {}

  const admin = yield* Admin.Admin
  const job = yield* admin.getJobById(id).pipe(
    Effect.orDie,
    Effect.filterOrFail((job) => job.status === "COMPLETED", (job) => new JobIncompleteError({ job })),
    Effect.retry({
      times: attempts,
      schedule: Schedule.spaced(interval),
      while: (error) => error instanceof JobIncompleteError,
    }),
  )

  return job
})

/**
 * Creates a test environment layer that connects to externally managed infrastructure.
 *
 * This layer reads connection URLs from environment variables and creates client layers
 * for Admin, JsonLines, ArrowFlight, and EvmRpc services. It does not spawn any processes.
 *
 * Environment variables:
 * - AMP_ADMIN_URL: Admin API URL (default: http://localhost:1610)
 * - AMP_JSONL_URL: JSON Lines API URL (default: http://localhost:1603)
 * - AMP_FLIGHT_URL: Arrow Flight API URL (default: http://localhost:1602)
 * - ANVIL_RPC_URL: Anvil RPC URL (default: http://localhost:8545)
 *
 * @returns A layer for the test environment with all necessary client services.
 */
export const layer = Vitest.layer(
  Effect.gen(function*() {
    const {
      adminUrl,
      anvilRpcUrl,
      flightUrl,
      jsonlUrl,
    } = yield* Config.all({
      adminUrl: Config.string("AMP_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      jsonlUrl: Config.string("AMP_JSONL_URL").pipe(Config.withDefault("http://localhost:1603")),
      flightUrl: Config.string("AMP_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
      anvilRpcUrl: Config.string("ANVIL_RPC_URL").pipe(Config.withDefault("http://localhost:8545")),
    })

    const flight = ArrowFlight.layer(createGrpcTransport({ baseUrl: flightUrl }))
    const jsonl = JsonLines.layer(jsonlUrl)
    const admin = Admin.layer(adminUrl)
    const rpc = EvmRpc.layer(anvilRpcUrl)

    return Layer.mergeAll(admin, jsonl, flight, rpc)
  }).pipe(
    Layer.unwrapEffect,
    Layer.merge(Fixtures.layer),
    Layer.provideMerge(NodeContext.layer),
  ),
  { excludeTestServices: true },
)
