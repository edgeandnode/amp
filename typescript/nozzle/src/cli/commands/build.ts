import { Command, Options } from "@effect/cli"
import { FileSystem, Path } from "@effect/platform"
import { Console, Effect, Option, Schema } from "effect"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as Model from "../../Model.js"

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
  })).pipe(
    Command.withDescription("Build a manifest from a dataset definition"),
    Command.provide(ManifestBuilder.ManifestBuilder.Default),
    Command.provide(ConfigLoader.ConfigLoader.Default)
  )
