import * as NodeContext from "@effect/platform-node/NodeContext"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Anvil from "nozzl/Anvil"
import * as Model from "nozzl/Model"
import * as Nozzle from "nozzl/Nozzle"
import * as Fixtures from "./Fixtures.ts"

export interface TestingOptions {
  /**
   * The path to the nozzle executable.
   *
   * @default "nozzle"
   */
  readonly nozzleExecutable?: string | undefined

  /**
   * Additional arguments to pass to the nozzle executable.
   *
   * This can be useful when running the nozzle server through cargo
   * directly from source e.g. during development or testing.
   */
  readonly nozzleArgs?: Array<string> | undefined

  /**
   * The working directory to run the anvil instance in.
   */
  readonly anvilWorkingDirectory?: string | undefined

  /**
   * The port to run the anvil instance on.
   *
   * @default 8545
   */
  readonly anvilPort?: number | undefined

  /**
   * Whether to print the stdout and stderr output of anvil to the console.
   *
   * @default "both"
   */
  readonly anvilOutput?: "stdout" | "stderr" | "both" | "none" | undefined

  /**
   * Whether to print the stdout and stderr output of nozzle to the console.
   *
   * @default "both"
   */
  readonly nozzleOutput?: "stdout" | "stderr" | "both" | "none" | undefined

  /**
   * The port to run the admin service on.
   *
   * @default 1610
   */
  readonly adminPort?: number | undefined

  /**
   * The port to run the json-lines service on.
   *
   * @default 1603
   */
  readonly jsonLinesPort?: number | undefined

  /**
   * The port to run the arrow-flight service on.
   *
   * @default 1602
   */
  readonly arrowFlightPort?: number | undefined
}

/**
 * The default options for the testing layer.
 */
export const defaultOptions: TestingOptions = {
  anvilOutput: "both",
  anvilPort: 8545,
  adminPort: 1610,
  arrowFlightPort: 1602,
  jsonLinesPort: 1603,
  nozzleOutput: "both",
  nozzleArgs: [],
  nozzleExecutable: "nozzle",
  anvilWorkingDirectory: undefined,
}

/**
 * Creates a test environment layer.
 *
 * @param config - The configuration for the test environment.
 * @returns A layer for the test environment.
 */
export const layer = (config: TestingOptions = {}) =>
  Effect.gen(function*() {
    const path = yield* Path.Path

    const {
      adminPort,
      anvilOutput,
      anvilPort,
      arrowFlightPort,
      jsonLinesPort,
      nozzleOutput,
    } = { ...defaultOptions, ...config }

    const anvil = Anvil.layer({
      httpPort: anvilPort,
      printOutput: anvilOutput,
      workingDirectory: Option.fromNullable(config.anvilWorkingDirectory).pipe(
        Option.getOrElse(() => path.resolve(import.meta.dirname, "..", "..", "..", "example", "contracts")),
      ),
    })

    const nozzle = Nozzle.layer({
      nozzleExecutable: Option.fromNullable(config.nozzleExecutable).pipe(
        Option.getOrElse(() =>
          path.resolve(import.meta.dirname, "..", "..", "..", "..", "..", "target", "release", "nozzle")
        ),
      ),
      nozzleArgs: config.nozzleArgs,
      printOutput: nozzleOutput,
      adminPort,
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
