import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile, manifestFile, registryUrl } from "../common.ts"

export const deploy = Command.make("deploy", {
  args: {
    dataset: Args.text({ name: "dataset" }).pipe(
      Args.withDescription("The name and version of the dataset to dump"),
      Args.withSchema(Schema.Union(Model.DatasetName, Model.DatasetNameAndVersion)),
      Args.optional,
    ),
    manifestFile: manifestFile.pipe(Options.optional),
    configFile: configFile.pipe(Options.optional),
    registryUrl,
    adminUrl,
  },
}).pipe(
  Command.withDescription("Deploy a dataset"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const manifest = yield* ManifestContext.ManifestContext.pipe(Effect.serviceOption)
      const admin = yield* Admin.Admin
      const dataset = yield* Option.match(args.dataset, {
        onSome: (dataset) => Effect.succeed(dataset),
        onNone: () =>
          manifest.pipe(Effect.map((manifest) => `${manifest.name}@${manifest.version}` as const), Effect.orDie),
      })

      const [name, version] = dataset.split("@")
      yield* admin.deployDataset(name, version)

      yield* Console.log(`Deployed dataset ${dataset}`)
    }),
  ),
  Command.provide(({ args }) =>
    Option.match(args.dataset, {
      onSome: () => Layer.empty,
      onNone: () =>
        ManifestContext.layerFromFile({ manifest: args.manifestFile, config: args.configFile }).pipe(
          Layer.provide(Registry.layer(`${args.registryUrl}`)),
        ),
    }).pipe(Layer.merge(Admin.layer(`${args.adminUrl}`)))
  ),
)
