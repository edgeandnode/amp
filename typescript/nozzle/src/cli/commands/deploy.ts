import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as ManifestDeployer from "../../ManifestDeployer.ts"

export const deploy = Command.make("deploy", {
  args: {
    dataset: Args.text({ name: "dataset" }).pipe(
      Args.optional,
      Args.withDescription(
        "The name of the dataset directory that contains the config. Dataset name must match the directory name.",
      ),
    ),
    config: Options.file("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to deploy"),
    ),
    manifest: Options.file("manifest").pipe(
      Options.optional,
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to deploy"),
    ),
    admin: Options.text("admin-url").pipe(
      Options.withFallbackConfig(Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610"))),
      Options.withDescription("The url of the Nozzle admin server"),
      Options.withSchema(Schema.URL),
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
  Command.withDescription("Deploy a dataset definition or manifest to Nozzle"),
  Command.withHandler(
    Effect.fn(function*() {
      const manifest = yield* ManifestContext.ManifestContext
      const deployer = yield* ManifestDeployer.ManifestDeployer
      const result = yield* deployer.deploy(manifest)
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromFile({ manifest: args.manifest, config: args.config, dataset: args.dataset }).pipe(
      Layer.merge(ManifestDeployer.ManifestDeployer.Default),
      Layer.provide(Admin.layer(`${args.admin}`)),
      Layer.provide(Registry.layer(`${args.registry}`)),
    )
  ),
)
