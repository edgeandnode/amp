import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Anvil from "nozzl/Anvil"
import * as Model from "nozzl/Model"
import * as Nozzle from "nozzl/Nozzle"
import * as Fixtures from "./Fixtures.ts"

/**
 * Creates a test environment layer.
 *
 * @param config - The configuration for the test environment.
 * @returns A layer for the test environment.
 */
export const layer = (
  config: {
    /**
     * The path to the nozzle executable.
     *
     * @default "nozzle"
     */
    nozzleExecutable?: string | undefined
    /**
     * Additional arguments to pass to the nozzle executable.
     *
     * This can be useful when running the nozzle server through cargo
     * directly from source e.g. during development or testing.
     */
    nozzleArgs?: Array<string> | undefined
    /**
     * The working directory to run the anvil instance in.
     */
    anvilWorkingDirectory?: string | undefined
    /**
     * The port to run the anvil instance on.
     *
     * @default 8545
     */
    anvilPort?: number | undefined
    /**
     * Whether to print the stdout and stderr output of anvil to the console.
     *
     * @default "both"
     */
    anvilOutput?: "stdout" | "stderr" | "both" | "none" | undefined
    /**
     * Whether to print the stdout and stderr output of nozzle to the console.
     *
     * @default "both"
     */
    nozzleOutput?: "stdout" | "stderr" | "both" | "none" | undefined
    /**
     * The port to run the admin service on.
     *
     * @default 1610
     */
    adminPort?: number | undefined
    /**
     * The port to run the registry service on.
     *
     * @default 1611
     */
    registryPort?: number | undefined
    /**
     * The port to run the json-lines service on.
     *
     * @default 1603
     */
    jsonLinesPort?: number | undefined
    /**
     * The port to run the arrow-flight service on.
     *
     * @default 1604
     */
    arrowFlightPort?: number | undefined
  } = {},
) =>
  Effect.gen(function*() {
    const path = yield* Path.Path

    const {
      adminPort = 1610,
      anvilOutput = "both",
      anvilPort = 8545,
      arrowFlightPort = 1604,
      jsonLinesPort = 1603,
      nozzleOutput = "both",
      registryPort = 1611,
    } = config

    const anvil = Anvil.layer({
      httpPort: anvilPort,
      printOutput: anvilOutput,
      workingDirectory: Option.fromNullable(config.anvilWorkingDirectory).pipe(
        Option.getOrElse(() => path.resolve(import.meta.dirname, "..", "..", "..", "example")),
      ),
    })

    const nozzle = Nozzle.layer({
      nozzleExecutable: config.nozzleExecutable,
      nozzleArgs: config.nozzleArgs,
      printOutput: nozzleOutput,
      adminPort,
      registryPort,
      jsonLinesPort,
      arrowFlightPort,
      providerDefinitions: {
        anvil: new Model.EvmRpcProvider({
          kind: "evm-rpc",
          network: "anvil",
          url: new URL(`http://localhost:${anvilPort}`),
        }),
      },
    })

    return Layer.merge(nozzle, anvil)
  }).pipe(
    Layer.unwrapEffect,
    Layer.merge(Fixtures.layer),
    Layer.provideMerge(NodeContext.layer),
  )
