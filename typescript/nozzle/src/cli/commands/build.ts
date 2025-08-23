import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"

export const build = Command.make("build", {
  args: {
    dataset: Args.text({ name: "dataset" }).pipe(
      Args.optional,
      Args.withDescription(
        "The name of the dataset directory that contains the config. Dataset name must match the directory name.",
      ),
    ),
    config: Options.file("config", { exists: "yes" }).pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to build to a manifest"),
    ),
    output: Options.file("output", { exists: "either" }).pipe(
      Options.optional,
      Options.withAlias("o"),
      Options.withDescription("The output file to write the manifest to"),
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the Nozzle registry server"),
      Options.withSchema(Schema.URL),
    ),
  },
}).pipe(
  Command.withDescription("Build a manifest from a dataset definition"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const json = yield* ManifestContext.ManifestContext.pipe(
        Effect.flatMap(Schema.encode(Model.DatasetManifest)),
        Effect.map((manifest) => JSON.stringify(manifest, null, 2)),
      )

      yield* Option.match(args.output, {
        onNone: () => Console.log(json),
        onSome: (output) =>
          fs
            .writeFileString(path.resolve(output), json)
            .pipe(Effect.tap(() => Console.log(`Manifest written to ${output}`))),
      })
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.config, args.dataset).pipe(
      Layer.provide(Registry.layer(`${args.registry}`)),
    )
  ),
)
