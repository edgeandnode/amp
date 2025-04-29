import { Command, Options } from "@effect/cli"
import { FileSystem, Path } from "@effect/platform"
import { Config, Console, Effect, Layer, Option, Schema } from "effect"
import * as Api from "../../Api.js"
import * as ManifestContext from "../../ManifestContext.js"
import * as Model from "../../Model.js"

export const build = Command.make("build", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to build to a manifest"),
    ),
    output: Options.text("output").pipe(
      Options.optional,
      Options.withAlias("o"),
      Options.withDescription("The output file to write the manifest to"),
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the Nozzle registry server"),
    ),
  },
}).pipe(
  Command.withDescription("Build a manifest from a dataset definition"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const json = yield* ManifestContext.ManifestContext.pipe(
        Effect.flatMap(Schema.encode(Model.DatasetManifest)),
        Effect.map((manifest) => JSON.stringify(manifest, null, 2)),
      )

      yield* Option.match(args.output, {
        onNone: () => Console.log(json),
        onSome: (output) =>
          fs.writeFileString(path.resolve(output), json).pipe(
            Effect.tap(() => Console.log(`Manifest written to ${output}`)),
          ),
      })
    })
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.config).pipe(
      Layer.provide(Api.Registry.withUrl(args.registry)),
    )
  ),
)
