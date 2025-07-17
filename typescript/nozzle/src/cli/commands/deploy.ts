import { Command, Options } from "@effect/cli"
import { Config, Console, Effect, Layer, Schema } from "effect"
import * as Api from "../../Api.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as ManifestDeployer from "../../ManifestDeployer.ts"

export const deploy = Command.make("deploy", {
  args: {
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
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
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
  Command.withHandler(() =>
    Effect.gen(function*() {
      const manifest = yield* ManifestContext.ManifestContext
      const deployer = yield* ManifestDeployer.ManifestDeployer
      const result = yield* deployer.deploy(manifest)
      yield* Console.log(result)
    })
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromFile({ manifest: args.manifest, config: args.config }).pipe(
      Layer.merge(ManifestDeployer.ManifestDeployer.Default),
      Layer.provide(Api.Admin.withUrl(`${args.admin}`)),
      Layer.provide(Api.Registry.withUrl(`${args.registry}`)),
    )
  ),
)
