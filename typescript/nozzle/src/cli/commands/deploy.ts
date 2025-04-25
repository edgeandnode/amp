import { Command, Options } from "@effect/cli"
import { Config, Effect, Layer } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"
import * as ManifestLoader from "../../ManifestLoader.js"

export const deploy = Command.make("deploy", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to deploy"),
    ),
    manifest: Options.text("manifest").pipe(
      Options.optional,
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to deploy"),
    ),
    admin: Options.text("admin-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
      Options.withDescription("The url of the Nozzle admin server"),
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the Nozzle registry server"),
    ),
  },
}).pipe(
  Command.withDescription("Deploy a dataset definition or manifest to Nozzle"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const deployer = yield* ManifestDeployer.ManifestDeployer
      const manifest = yield* ConfigLoader.loadManifestOrConfig(args.manifest, args.config)
      const result = yield* deployer.deploy(manifest)
      yield* Effect.log(result)
    })
  ),
  Command.provide(({ args }) =>
    Layer.mergeAll(
      ManifestDeployer.ManifestDeployer.Default,
      ManifestBuilder.ManifestBuilder.Default,
      ManifestLoader.ManifestLoader.Default,
      ConfigLoader.ConfigLoader.Default,
    ).pipe(Layer.provide(Layer.mergeAll(
      Api.Admin.withUrl(args.admin),
      Api.Registry.withUrl(args.registry),
    )))
  ),
)
