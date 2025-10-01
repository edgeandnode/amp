import { createGrpcTransport } from "@connectrpc/connect-node"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Vitest from "@effect/vitest"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Admin from "nozzl/api/Admin"
import * as ArrowFlight from "nozzl/api/ArrowFlight"
import * as JsonLines from "nozzl/api/JsonLines"
import * as EvmRpc from "nozzl/evm/EvmRpc"
import * as Fixtures from "./Fixtures.ts"

/**
 * Creates a test environment layer that connects to externally managed infrastructure.
 *
 * This layer reads connection URLs from environment variables and creates client layers
 * for Admin, JsonLines, ArrowFlight, and EvmRpc services. It does not spawn any processes.
 *
 * Environment variables:
 * - NOZZLE_ADMIN_URL: Admin API URL (default: http://localhost:1610)
 * - NOZZLE_JSONL_URL: JSON Lines API URL (default: http://localhost:1603)
 * - NOZZLE_FLIGHT_URL: Arrow Flight API URL (default: http://localhost:1602)
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
      adminUrl: Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      jsonlUrl: Config.string("NOZZLE_JSONL_URL").pipe(Config.withDefault("http://localhost:1603")),
      flightUrl: Config.string("NOZZLE_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
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
