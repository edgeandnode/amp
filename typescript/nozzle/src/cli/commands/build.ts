import { Command, Options } from "@effect/cli"
import { FileSystem, Path } from "@effect/platform"
import { Config, Console, Effect, Layer, Option, Schema } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as Model from "../../Model.js"

const layer = Layer.mergeAll(
  ManifestBuilder.ManifestBuilder.Default,
  ConfigLoader.ConfigLoader.Default
)

export const build = Command.make("build", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to build to a manifest")
    ),
    output: Options.text("output").pipe(
      Options.optional,
      Options.withAlias("o"),
      Options.withDescription("The output file to write the manifest to")
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611"))
      ),
      Options.withDescription("The url of the Nozzle registry server")
    )
  }
}, ({ args }) =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const fs = yield* FileSystem.FileSystem
    const config = yield* ConfigLoader.ConfigLoader
    const builder = yield* ManifestBuilder.ManifestBuilder

    const definition = yield* Option.match(args.config, {
      onSome: (file) => config.load(file).pipe(Effect.map(Option.some)),
      onNone: () => config.find()
    }).pipe(Effect.flatten, Effect.orDie)

    const json = yield* builder.build(definition).pipe(
      Effect.flatMap(Schema.encode(Model.DatasetManifest)),
      Effect.map((manifest) => JSON.stringify(manifest, null, 2)),
      Effect.orDie
    )

    yield* Option.match(args.output, {
      onNone: () => Console.log(json),
      onSome: (output) =>
        fs.writeFileString(path.resolve(output), json).pipe(
          Effect.tap(() => Effect.log(`Manifest written to ${output}`)),
          Effect.orDie
        )
    })
  }).pipe(Effect.provide(layer.pipe(
    Layer.provide(Api.layerRegistry(args.registry))
  )))).pipe(
    Command.withDescription("Build a manifest from a dataset definition")
  )
