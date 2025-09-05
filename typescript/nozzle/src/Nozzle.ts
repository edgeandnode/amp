import { createGrpcTransport } from "@connectrpc/connect-node"
import * as Command from "@effect/platform/Command"
import type * as CommandExecutor from "@effect/platform/CommandExecutor"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as String from "effect/String"
import * as Admin from "./api/Admin.ts"
import * as ArrowFlight from "./api/ArrowFlight.ts"
import * as JsonLines from "./api/JsonLines.ts"
import * as Registry from "./api/Registry.ts"
import * as Model from "./Model.ts"
import * as Utils from "./Utils.ts"

/**
 * Error type for the nozzle service.
 */
export class NozzleError extends Schema.TaggedError<NozzleError>("NozzleError")("NozzleError", {
  cause: Schema.Unknown.pipe(Schema.optional),
  message: Schema.String,
}) {}

/**
 * Service definition for the nozzle service.
 */
export class Nozzle extends Context.Tag("Nozzle/Nozzle")<Nozzle, {
  /**
   * The stdout stream of the nozzle process.
   */
  readonly stdout: Stream.Stream<string, NozzleError>

  /**
   * The stderr stream of the nozzle process.
   */
  readonly stderr: Stream.Stream<string, NozzleError>

  /**
   * Joins the fiber that runs the nozzle process.
   *
   * @returns An effect that completes when the process exits.
   */
  readonly join: Effect.Effect<void, NozzleError>

  /**
   * Kills the nozzle process.
   *
   * @param signal - The signal to send to the process.
   * @returns An effect that completes when the process is killed.
   */
  readonly kill: (signal?: CommandExecutor.Signal) => Effect.Effect<void>
}>() {}

/**
 * The configuration for the nozzle instance.
 */
export interface NozzleOptions {
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
   * The provider definitions to configure nozzle with.
   */
  readonly providerDefinitions?: Record<string, typeof Model.Provider.Type>

  /**
   * The temporary directory to store data and configuration.
   */
  readonly tempDirectory?: string | undefined

  /**
   * The logging level to use for nozzle.
   *
   * @default "info"
   */
  readonly loggingLevel?: "error" | "warn" | "info" | "debug" | "trace" | undefined

  /**
   * Whether to print the stdout and stderr output of nozzle to the console.
   *
   * @default "none"
   */
  readonly printOutput?: "stdout" | "stderr" | "both" | "none" | undefined

  /**
   * The port to run the admin service on.
   *
   * @default 1610
   */
  readonly adminPort?: number | undefined

  /**
   * The port to run the registry service on.
   *
   * @default 1611
   */
  readonly registryPort?: number | undefined

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
 * Creates a nozzle service instance.
 */
export const make = Effect.fn(function*(options: NozzleOptions = {}) {
  const path = yield* Path.Path
  const fs = yield* FileSystem.FileSystem
  const {
    adminPort = 1610,
    arrowFlightPort = 1602,
    jsonLinesPort = 1603,
    loggingLevel = "info",
    nozzleArgs = [],
    nozzleExecutable = "nozzle",
    printOutput = "none",
    providerDefinitions = [],
    registryPort = 1611,
    tempDirectory = undefined,
  } = options

  // Create a temporary directory for the nozzle data or use the provided directory.
  const directory = yield* Option.fromNullable(tempDirectory).pipe(
    Option.match({
      onNone: () => fs.makeTempDirectoryScoped({ prefix: "nozzle-" }),
      onSome: (directory) => Effect.succeed(directory),
    }),
  )

  yield* Effect.all([
    fs.makeDirectory(path.join(directory, "data")),
    fs.makeDirectory(path.join(directory, "datasets")),
    fs.makeDirectory(path.join(directory, "providers")),
  ]).pipe(Effect.orDie)

  const nozzleConfig = `
    |data_dir = "data"
    |dataset_defs_dir = "datasets"
    |providers_dir = "providers"
    |max_mem_mb = 2000
    |spill_location = []
    |flight_addr = "0.0.0.0:${arrowFlightPort}"
    |jsonl_addr = "0.0.0.0:${jsonLinesPort}"
    |registry_service_addr = "0.0.0.0:${registryPort}"
    |admin_api_addr = "0.0.0.0:${adminPort}"
  `

  // Write the nozzle config and provider configs.
  yield* fs.writeFileString(path.join(directory, "config.toml"), String.stripMargin(nozzleConfig))
  for (const [name, definition] of Object.entries(providerDefinitions)) {
    const config = yield* providerConfig(definition)
    yield* fs.writeFileString(path.join(directory, "providers", `${name}.toml`), config)
  }

  const cmd = Command.make(nozzleExecutable, ...nozzleArgs, "server", "--dev").pipe(
    Command.env({
      NOZZLE_CONFIG: path.join(directory, "config.toml"),
      NOZZLE_LOG: loggingLevel,
    }),
  )

  // This effect starts the server in the background.
  const nozzle = yield* Command.start(cmd).pipe(
    Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
  )

  const stdout = yield* Utils.withLinePrefix(nozzle.stdout, "[NOZZLE]").pipe(
    Stream.mapError((cause) => new NozzleError({ message: "Failed to read stdout", cause })),
    Stream.broadcastDynamic({ capacity: "unbounded" }),
  )

  const stderr = yield* Utils.withLinePrefix(nozzle.stderr, "[NOZZLE]").pipe(
    Stream.mapError((cause) => new NozzleError({ message: "Failed to read stderr", cause })),
    Stream.broadcastDynamic({ capacity: "unbounded" }),
  )

  // Print the stdout and stderr output of nozzle to the console if configured to do so.
  yield* Utils.intoNodeSink({ which: printOutput, stdout, stderr })

  // This effect waits for all ports to be open.
  const open = Effect.all([
    Utils.waitForPort(adminPort),
    Utils.waitForPort(registryPort),
    Utils.waitForPort(jsonLinesPort),
    Utils.waitForPort(arrowFlightPort),
  ], {
    concurrency: "unbounded",
  }).pipe(
    Effect.timeout("10 seconds"),
    Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
  )

  const exit = nozzle.exitCode.pipe(
    Effect.mapError((cause) => new NozzleError({ cause, message: "Process interrupted" })),
    Effect.filterOrFail(
      (code) => code === 0,
      (code) => new NozzleError({ message: `Process failed with code ${code}` }),
    ),
  )

  // Wait for the server to either be up and running with all ports open or exit (crash).
  yield* Effect.raceFirst(
    exit.pipe(Effect.as(new NozzleError({ message: "Process finished prematurely" }))),
    open,
  )

  return {
    stdout,
    stderr,
    join: exit.pipe(Effect.asVoid),
    kill: Effect.fn(function*(signal?: CommandExecutor.Signal) {
      return yield* nozzle.kill(signal).pipe(Effect.orDie)
    }),
  }
})

/**
 * Creates a nozzle service layer.
 */
export const layer = (options: NozzleOptions = {}) => {
  const {
    adminPort = 1610,
    arrowFlightPort = 1602,
    jsonLinesPort = 1603,
    registryPort = 1611,
  } = options

  return make(options).pipe(
    Layer.scoped(Nozzle),
    Layer.merge(Admin.layer(`http://localhost:${adminPort}`)),
    Layer.merge(Registry.layer(`http://localhost:${registryPort}`)),
    Layer.merge(JsonLines.layer(`http://localhost:${jsonLinesPort}`)),
    Layer.merge(ArrowFlight.layer(createGrpcTransport({ baseUrl: `http://localhost:${arrowFlightPort}` }))),
  )
}

/**
 * Renders a provider definition to a config string.
 *
 * @param definition - The provider definition to render.
 * @returns The rendered provider config.
 */
const providerConfig = Effect.fn(function*(definition: typeof Model.Provider.Type) {
  const encoded = yield* Schema.encode(Model.Provider)(definition)
  return Match.value(encoded.kind).pipe(
    Match.when("evm-rpc", () => {
      const config = `
        |kind = "evm-rpc"
        |network = "${encoded.network}"
        |url = "${encoded.url}"
      `

      return String.stripMargin(config)
    }),
    Match.exhaustive,
  )
})
