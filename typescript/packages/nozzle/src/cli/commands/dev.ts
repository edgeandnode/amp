import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Anvil from "../../Anvil.ts"
import * as ConfigLoader from "../../ConfigLoader.ts"
import * as DevServer from "../../DevServer.ts"
import * as Model from "../../Model.ts"
import * as Nozzle from "../../Nozzle.ts"

export const dev = Command.make("dev", {
  args: {
    executable: Options.text("executable").pipe(
      Options.withDescription("The path to the nozzle executable"),
      Options.withDefault("nozzle"),
    ),
    directory: Options.directory("directory", { exists: "no" }).pipe(
      Options.withDescription("The directory to use for temporary files"),
      Options.optional,
    ),
  },
}).pipe(
  Command.withDescription("Run a development server with hot reloading"),
  Command.withHandler(() =>
    Effect.gen(function*() {
      const anvil = yield* Anvil.Anvil
      const nozzle = yield* Nozzle.Nozzle
      const dev = DevServer.layer().pipe(
        Layer.provide(ConfigLoader.ConfigLoader.Default),
      )

      yield* Effect.all([
        Layer.launch(dev).pipe(Effect.tap(() => Effect.log("server"))),
        nozzle.join.pipe(Effect.tap(() => Effect.log("nozzle"))),
        anvil.join.pipe(Effect.tap(() => Effect.log("anvil"))),
      ], { concurrency: "unbounded" })
    }).pipe(Effect.scoped)
  ),
  Command.provide(({ args }) =>
    Layer.unwrapScoped(Effect.gen(function*() {
      const path = yield* Path.Path
      const fs = yield* FileSystem.FileSystem

      // Use the provided directory or default to ".nozzle"
      const directory = yield* Option.match(args.directory, {
        onSome: (dir) => Effect.succeed(path.resolve(dir)),
        onNone: () => Effect.succeed(path.resolve(".nozzle")),
      })

      // TODO: There's a weird bug sometimes here where the directory is not cleaned up properly.
      yield* Effect.acquireRelease(fs.makeDirectory(directory), () =>
        fs.remove(directory, { recursive: true }).pipe(Effect.ignore))

      const anvil = Anvil.layer({
        httpPort: 8545,
        printOutput: "both",
        verbosity: 0,
        workingDirectory: directory,
      })

      const nozzle = Nozzle.layer({
        nozzleExecutable: args.executable,
        printOutput: "both",
        tempDirectory: directory,
        loggingLevel: "warn",
        providerDefinitions: {
          anvil: new Model.EvmRpcProvider({
            kind: "evm-rpc",
            network: "anvil",
            url: new URL("http://localhost:8545"),
          }),
        },
      })

      return Layer.merge(anvil, nozzle)
    }))
  ),
)
