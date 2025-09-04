import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"

export const deploy = Command.make("deploy", {
  args: {
    dataset: Args.text({ name: "dataset" }).pipe(
      Args.withDescription("The name and version of the dataset to dump"),
      Args.withSchema(Schema.Union(Model.DatasetName, Model.DatasetNameAndVersion)),
      Args.optional,
    ),
    manifest: Options.file("manifest").pipe(
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to dump"),
      Options.optional,
    ),
    config: Options.file("config").pipe(
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to dump"),
      Options.optional,
    ),
    registryUrl: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the registry server"),
      Options.withSchema(Schema.URL),
    ),
    adminUrl: Options.text("admin-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
      Options.withDescription("The Admin API URL to use for the dump request"),
      Options.withSchema(Schema.URL),
    ),
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
        ManifestContext.layerFromFile({ manifest: args.manifest, config: args.config }).pipe(
          Layer.provide(Registry.layer(`${args.registryUrl}`)),
        ),
    }).pipe(Layer.merge(Admin.layer(`${args.adminUrl}`)))
  ),
)
