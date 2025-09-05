import * as Command from "@effect/platform/Command"
import type * as CommandExecutor from "@effect/platform/CommandExecutor"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as EvmRpc from "./evm/EvmRpc.ts"
import * as Model from "./Model.ts"
import * as Utils from "./Utils.ts"

// NODE: This is not a secret prviate key, it's one of the test keys from anvil's default mnemonic.
const DEFAULT_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

/**
 * Error type for the anvil service.
 */
export class AnvilError extends Schema.TaggedError<AnvilError>("AnvilError")("AnvilError", {
  cause: Schema.Unknown.pipe(Schema.optional),
  message: Schema.String,
}) {}

/**
 * Service definition for the anvil service.
 */
export class Anvil extends Context.Tag("Nozzle/Anvil")<Anvil, {
  /**
   * Joins the fiber that runs the anvil process.
   *
   * @returns An effect that completes when the process exits.
   */
  readonly join: Effect.Effect<void, AnvilError>

  /**
   * Kills the anvil instance.
   *
   * @param signal - The signal to send to the process.
   * @returns An effect that completes when the process is killed.
   */
  readonly kill: (signal?: CommandExecutor.Signal) => Effect.Effect<void>

  /**
   * Runs a script on the anvil instance.
   *
   * @param script - The script to run.
   * @returns An effect that completes when the script exits.
   */
  readonly runScript: (script: string) => Effect.Effect<void, AnvilError>
}>() {}

/**
 * The configuration for the anvil service.
 */
export interface AnvilOptions {
  /**
   * The verbosity level to run anvil with.
   */
  readonly verbosity?: 0 | 1 | 2 | 3 | 4 | 5

  /**
   * The port to run the anvil instance on.
   */
  readonly httpPort?: number | undefined

  /**
   * Whether to print the stdout and stderr output of anvil to the console.
   *
   * @default "none"
   */
  readonly printOutput?: "stdout" | "stderr" | "both" | "none" | undefined

  /**
   * The working directory to run the anvil instance in.
   */
  readonly workingDirectory?: string | undefined
}

/**
 * The options for running a script on the anvil instance.
 */
export interface ScriptOptions {
  /**
   * The private key to use for the script.
   */
  readonly privateKey?: string | undefined

  /**
   * The working directory to run the script in.
   */
  readonly workingDirectory?: string | undefined
}

/**
 * Creates an anvil service instance.
 *
 * @param config - The configuration for the anvil service.
 * @returns An anvil service instance.
 */
export const make = Effect.fn(function*(options: AnvilOptions = {}) {
  const parent = yield* Effect.context<CommandExecutor.CommandExecutor>()
  const fs = yield* FileSystem.FileSystem
  const {
    httpPort = 8545,
    printOutput = "none",
    workingDirectory = undefined,
  } = options

  const directory = yield* Option.fromNullable(workingDirectory).pipe(
    Option.match({
      onSome: (_) => Effect.succeed(_),
      onNone: () => fs.makeTempDirectory({ prefix: "anvil-" }),
    }),
  )

  const verbosity = Match.value(options.verbosity).pipe(
    Match.when(0, () => ["--quiet"] as const),
    Match.whenOr(1, undefined, () => [] as const),
    Match.whenOr(2, 3, 4, 5, (_) => ["--verbosity", `${_}`] as const),
    Match.exhaustive,
  )

  const cmd = Command.make("anvil", "--port", `${httpPort}`, ...verbosity).pipe(
    Command.workingDirectory(directory),
    Command.stderr("inherit"),
  )

  const anvil = yield* Command.start(cmd).pipe(
    Effect.mapError((cause) => new AnvilError({ message: "Failed to start", cause })),
  )

  const stdout = yield* Utils.withLinePrefix(anvil.stdout, "[ANVIL]").pipe(
    Stream.mapError((cause) => new AnvilError({ message: "Failed to read stdout", cause })),
    Stream.broadcastDynamic({ capacity: "unbounded" }),
  )

  const stderr = yield* Utils.withLinePrefix(anvil.stderr, "[ANVIL]").pipe(
    Stream.mapError((cause) => new AnvilError({ message: "Failed to read stderr", cause })),
    Stream.broadcastDynamic({ capacity: "unbounded" }),
  )

  // Print the stdout and stderr output of nozzle to the console if configured to do so.
  yield* Utils.intoNodeSink({ which: printOutput, stdout, stderr })

  // Checks if anvil exited prematurely.
  const exit = anvil.exitCode.pipe(
    Effect.mapError((cause) => new AnvilError({ cause, message: "Process interrupted" })),
    Effect.filterOrFail(
      (code) => code === 0,
      (code) => new AnvilError({ message: `Process failed with code ${code}` }),
    ),
  )

  // Tries to connect to the port to chcek if it's actually open.
  const open = Utils.waitForPort(httpPort).pipe(
    Effect.timeout("10 seconds"),
    Effect.mapError((cause) => new AnvilError({ message: "Failed to check port", cause })),
  )

  // Wait for the server to either be up and running with all ports open or exit (crash).
  yield* Effect.raceFirst(
    exit.pipe(Effect.as(new AnvilError({ message: "Process finished prematurely" }))),
    open,
  )

  return {
    stdout,
    stderr,
    join: exit.pipe(Effect.asVoid),
    kill: Effect.fn(function*(signal?: CommandExecutor.Signal) {
      return yield* anvil.kill(signal).pipe(Effect.orDie)
    }),
    runScript: (script: string, options: ScriptOptions = {}) =>
      Effect.gen(function*() {
        const { privateKey = DEFAULT_PRIVATE_KEY, workingDirectory = directory } = options

        const args = [
          ["--private-key", privateKey],
          ["--rpc-url", `http://localhost:${httpPort}`],
          ["--broadcast"],
          [script],
        ].flat()

        const cmd = Command.make("forge", "script", ...args).pipe(Command.workingDirectory(workingDirectory))
        const forge = yield* Command.start(cmd).pipe(
          Effect.mapError((cause) => new AnvilError({ message: "Script failed to run", cause })),
        )

        const stdout = yield* Utils.withLinePrefix(forge.stdout, "[FORGE]").pipe(
          Stream.mapError((cause) => new AnvilError({ message: "Failed to read stdout", cause })),
          Stream.broadcastDynamic({ capacity: "unbounded" }),
        )

        const stderr = yield* Utils.withLinePrefix(forge.stderr, "[FORGE]").pipe(
          Stream.mapError((cause) => new AnvilError({ message: "Failed to read stderr", cause })),
          Stream.broadcastDynamic({ capacity: "unbounded" }),
        )

        // Print the stdout and stderr output of nozzle to the console if configured to do so.
        yield* Utils.intoNodeSink({ which: printOutput, stdout, stderr })

        return yield* forge.exitCode.pipe(
          Effect.mapError((cause) => new AnvilError({ message: "Script failed to run", cause })),
          Effect.filterOrFail(
            (code) => code === 0,
            (code) => new AnvilError({ message: `Script failed with code ${code}` }),
          ),
          Effect.asVoid,
        )
      }).pipe(
        // TODO: Is this a good way to merge the parent context in here to prevent the `CommandExecutor` from being required locally?
        Effect.mapInputContext((context: Context.Context<Scope.Scope>) => Context.merge(parent, context)),
        Effect.scoped,
      ),
  }
})

/**
 * Creates an anvil service layer.
 *
 * @param config - The configuration for the anvil service.
 * @returns A layer for the anvil service.
 */
export const layer = (config: AnvilOptions = {}) =>
  make(config).pipe(Layer.scoped(Anvil), Layer.merge(EvmRpc.layer(`http://localhost:${config.httpPort}`)))

/**
 * The anvil dataset.
 */
export const dataset = new Model.DatasetRpc({
  "kind": "evm-rpc",
  "name": "anvil",
  "version": "0.1.0",
  "network": "anvil",
  "schema": {
    "blocks": {
      "timestamp": { "Timestamp": ["Nanosecond", "+00:00"] },
      "mix_hash": { "FixedSizeBinary": 32 },
      "base_fee_per_gas": { "Decimal128": [38, 0] },
      "blob_gas_used": "UInt64",
      "miner": { "FixedSizeBinary": 20 },
      "block_num": "UInt64",
      "parent_hash": { "FixedSizeBinary": 32 },
      "excess_blob_gas": "UInt64",
      "state_root": { "FixedSizeBinary": 32 },
      "receipt_root": { "FixedSizeBinary": 32 },
      "withdrawals_root": { "FixedSizeBinary": 32 },
      "hash": { "FixedSizeBinary": 32 },
      "difficulty": { "Decimal128": [38, 0] },
      "ommers_hash": { "FixedSizeBinary": 32 },
      "transactions_root": { "FixedSizeBinary": 32 },
      "nonce": "UInt64",
      "parent_beacon_root": { "FixedSizeBinary": 32 },
      "extra_data": "Binary",
      "gas_used": "UInt64",
      "logs_bloom": "Binary",
      "gas_limit": "UInt64",
    },
    "logs": {
      "topic3": { "FixedSizeBinary": 32 },
      "topic2": { "FixedSizeBinary": 32 },
      "tx_index": "UInt32",
      "tx_hash": { "FixedSizeBinary": 32 },
      "data": "Binary",
      "address": { "FixedSizeBinary": 20 },
      "block_hash": { "FixedSizeBinary": 32 },
      "block_num": "UInt64",
      "log_index": "UInt32",
      "topic0": { "FixedSizeBinary": 32 },
      "topic1": { "FixedSizeBinary": 32 },
      "timestamp": { "Timestamp": ["Nanosecond", "+00:00"] },
    },
    "transactions": {
      "gas_used": "UInt64",
      "from": { "FixedSizeBinary": 20 },
      "block_hash": { "FixedSizeBinary": 32 },
      "type": "Int32",
      "gas_limit": "UInt64",
      "timestamp": { "Timestamp": ["Nanosecond", "+00:00"] },
      "status": "Boolean",
      "s": "Binary",
      "tx_index": "UInt32",
      "gas_price": { "Decimal128": [38, 0] },
      "input": "Binary",
      "max_fee_per_gas": { "Decimal128": [38, 0] },
      "r": "Binary",
      "v": "Binary",
      "max_priority_fee_per_gas": { "Decimal128": [38, 0] },
      "max_fee_per_blob_gas": { "Decimal128": [38, 0] },
      "tx_hash": { "FixedSizeBinary": 32 },
      "to": { "FixedSizeBinary": 20 },
      "block_num": "UInt64",
      "nonce": "UInt64",
      "value": { "Decimal128": [38, 0] },
    },
  },
})
