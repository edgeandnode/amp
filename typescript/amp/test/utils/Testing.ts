import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Admin from "@edgeandnode/amp/api/Admin"
import * as ArrowFlight from "@edgeandnode/amp/api/ArrowFlight"
import * as JsonLines from "@edgeandnode/amp/api/JsonLines"
import * as EvmRpc from "@edgeandnode/amp/evm/EvmRpc"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Vitest from "@effect/vitest"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Fixtures from "./Fixtures.ts"

/**
 * Waits for a job to complete by polling its status.
 *
 * @param jobIdStr The job ID as a string returned from dump operations (can be plain number or JSON like {"job_id":1})
 * @param maxAttempts Maximum number of polling attempts (default: 30)
 * @param intervalMs Polling interval in milliseconds (default: 200ms)
 * @returns An effect that yields the completed JobInfo or fails if the job errors or times out
 */
export const waitForJobCompletion = (
  jobIdStr: string,
  maxAttempts = 30,
  intervalMs = 200,
) =>
  Effect.gen(function*() {
    const admin = yield* Admin.Admin

    // Parse job ID - handle both plain number strings and JSON objects like {"job_id":1}
    let jobId: number
    try {
      const parsed = JSON.parse(jobIdStr)
      if (typeof parsed === "object" && parsed !== null && "job_id" in parsed) {
        jobId = parsed.job_id
      } else {
        jobId = parsed
      }
    } catch {
      // If JSON parse fails, try to parse as plain number
      jobId = parseInt(jobIdStr, 10)
    }

    if (isNaN(jobId)) {
      return yield* Effect.die(new Error(`Invalid job ID: ${jobIdStr}`))
    }

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const job = yield* admin.getJobById(jobId)

      if (job.status === "COMPLETED") {
        return job
      } else if (job.status === "ERROR") {
        return yield* Effect.fail(new Error(`Job ${jobId} failed with ERROR status`))
      } else if (job.status === "STOPPED") {
        return yield* Effect.fail(new Error(`Job ${jobId} was stopped`))
      }

      // Job is still running, wait before next attempt
      if (attempt < maxAttempts - 1) {
        yield* Effect.sleep(`${intervalMs} millis`)
      }
    }

    // Timed out
    return yield* Effect.fail(new Error(`Job ${jobId} did not complete within ${maxAttempts * intervalMs}ms`))
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
