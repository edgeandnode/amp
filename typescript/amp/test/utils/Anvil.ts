import * as Model from "@edgeandnode/amp/Model"
import * as Command from "@effect/platform/Command"
import type * as CommandExecutor from "@effect/platform/CommandExecutor"
import * as Path from "@effect/platform/Path"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

/**
 * The anvil dataset manifest.
 *
 * This is loaded from the generated manifest file to ensure consistency with the CLI output.
 *
 * To regenerate the manifest, run:
 * ```bash
 * cargo run --release -p ampctl -- manifest generate --kind evm-rpc --network anvil --start-block 0 > typescript/amp/test/fixtures/anvil.json
 * ```
 */
import anvilManifest from "../fixtures/anvil.json" with { type: "json" }

/**
 * The anvil dataset
 */
export const dataset = Schema.decodeUnknownSync(Model.DatasetEvmRpc)(anvilManifest)

// NOTE: This is not a secret private key, it's one of the test keys from anvil's default mnemonic.
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

export class Anvil extends Context.Tag("Amp/Anvil")<Anvil, {
  /**
   * Runs a script on the anvil instance.
   *
   * @param script - The script to run.
   * @param options - Optional configuration for the script execution.
   * @returns An effect that completes when the script exits.
   */
  script: (script: string, options?: ScriptOptions) => Effect.Effect<void, AnvilError>
}>() {}

/**
 * Layer for the anvil service.
 */
export const layer = (rpcUrl: string) =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const context = yield* Effect.context<CommandExecutor.CommandExecutor>()

    const script = Effect.fn(function*(script: string, options: ScriptOptions = {}) {
      const {
        privateKey = DEFAULT_PRIVATE_KEY,
        workingDirectory = path.resolve(import.meta.dirname, "..", "fixtures", "contracts"),
      } = options

      const args = [
        ["--private-key", privateKey],
        ["--rpc-url", rpcUrl],
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
        Effect.mapInputContext((input: Context.Context<never>) => Context.merge(input, context)),
      )
    })

    return { script }
  }).pipe(Layer.effect(Anvil))
