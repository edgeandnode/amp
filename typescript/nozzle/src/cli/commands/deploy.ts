import { Command, Options } from "@effect/cli"
import { Config, Effect, Layer, Option, Unify } from "effect"
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
}, ({ args }) =>
  Effect.gen(function*() {
    const deployer = yield* ManifestDeployer.ManifestDeployer
    const loader = yield* ManifestLoader.ManifestLoader
    const config = yield* ConfigLoader.ConfigLoader
    const builder = yield* ManifestBuilder.ManifestBuilder
    const manifest = yield* Unify.unify(Option.match(args.manifest, {
      onSome: (file) => loader.load(file).pipe(Effect.map(Option.some)),
      onNone: () =>
        Unify.unify(Option.match(args.config, {
          onSome: (file) => config.load(file).pipe(Effect.flatMap(builder.build), Effect.map(Option.some)),
          onNone: () =>
            config.find().pipe(Effect.flatMap(Unify.unify(Option.match({
              onSome: (definition) => builder.build(definition).pipe(Effect.map(Option.some)),
              onNone: () => loader.load("nozzle.json").pipe(Effect.map(Option.some)),
            })))),
        })),
    })).pipe(
      Effect.orDie,
      Effect.flatMap(Option.match({
        onNone: () => Effect.dieMessage("No manifest or config file provided"),
        onSome: Effect.succeed,
      })),
    )

    const result = yield* deployer.deploy(manifest)
    yield* Effect.log(result)
  }).pipe(
    Effect.provide(
      layer.pipe(Layer.provide(Layer.mergeAll(
        Api.layerAdmin(args.admin),
        Api.layerRegistry(args.registry),
      ))),
    ),
  )).pipe(
    Command.withDescription("Deploy a dataset definition or manifest to Nozzle"),
  )

const layer = Layer.mergeAll(
  ManifestDeployer.ManifestDeployer.Default,
  ManifestBuilder.ManifestBuilder.Default,
  ManifestLoader.ManifestLoader.Default,
  ConfigLoader.ConfigLoader.Default,
)
