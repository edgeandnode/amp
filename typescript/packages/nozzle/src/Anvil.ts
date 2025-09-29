import * as Command from "@effect/platform/Command"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as EvmRpc from "./evm/EvmRpc.ts"
import * as Model from "./Model.ts"

// NODE: This is not a secret prviate key, it's one of the test keys from anvil's default mnemonic.
const DEFAULT_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

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
 * Error type for the anvil service.
 */
export class AnvilError extends Schema.TaggedError<AnvilError>("AnvilError")("AnvilError", {
  cause: Schema.Unknown.pipe(Schema.optional),
  message: Schema.String,
}) {}

/**
 * Runs a script on the anvil instance.
 *
 * @param script - The script to run.
 * @returns An effect that completes when the script exits.
 */
export const script = (script: string, options: ScriptOptions = {}) =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const rpc = yield* EvmRpc.EvmRpc
    const {
      privateKey = DEFAULT_PRIVATE_KEY,
      workingDirectory = path.resolve(import.meta.dirname, "..", "test", "fixtures", "contracts"),
    } = options

    const args = [
      ["--private-key", privateKey],
      ["--rpc-url", rpc.url],
      ["--broadcast"],
      [`${path.join("script", script)}`],
    ].flat()

    const cmd = Command.make("forge", "script", ...args).pipe(
      Command.workingDirectory(workingDirectory),
      Command.stderr("inherit"),
      Command.stdout("inherit"),
    )

    return yield* Command.exitCode(cmd).pipe(
      Effect.mapError((cause) => new AnvilError({ message: "Script failed to run", cause })),
      Effect.filterOrFail(
        (code) => code === 0,
        (code) => new AnvilError({ message: `Script failed with code ${code}` }),
      ),
      Effect.asVoid,
    )
  }).pipe(Effect.scoped)

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
