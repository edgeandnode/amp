import * as Command from "@effect/platform/Command"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as EvmRpc from "./evm/EvmRpc.ts"
import * as Model from "./Model.ts"

/**
 * The anvil dataset manifest.
 *
 * This is loaded from the generated manifest file to ensure consistency with the CLI output.
 *
 * To regenerate the manifest, run:
 * ```bash
 * cargo run --release -p ampctl -- gen-manifest --kind evm-rpc --name anvil --network anvil --start-block 0 > typescript/amp/test/fixtures/anvil_rpc.json
 * ```
 *
 * Then adjust version to 0.1.0
 */
import anvilManifest from "../test/fixtures/anvil_rpc.json" with { type: "json" }

/**
 * The anvil dataset.
 *
 * Note: We use decodeSync to validate the JSON structure matches the DatasetEvmRpc schema.
 * The type assertion is needed because JSON imports don't preserve literal types.
 */
export const dataset = Schema.decodeSync(Model.DatasetEvmRpc)(anvilManifest as any)

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
