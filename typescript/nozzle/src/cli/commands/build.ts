import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { configFile, registryUrl } from "../common.ts"

export const build = Command.make("build", {
  args: {
    config: configFile.pipe(Options.optional),
    output: Options.file("output", { exists: "either" }).pipe(
      Options.withAlias("o"),
      Options.withDescription("The output file to write the manifest to"),
      Options.optional,
    ),
    registryUrl,
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
          fs.writeFileString(path.resolve(output), json).pipe(
            Effect.tap(() => Console.log(`Manifest written to ${output}`)),
          ),
      })
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.config).pipe(Layer.provide(Registry.layer(`${args.registryUrl}`)))
  ),
)
