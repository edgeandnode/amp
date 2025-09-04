import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"

export const register = Command.make("register", {
  args: {
    config: Options.file("config").pipe(
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to register"),
      Options.optional,
    ),
    manifest: Options.file("manifest").pipe(
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to register"),
      Options.optional,
    ),
    registryUrl: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the registry server"),
      Options.withSchema(Schema.URL),
    ),
  },
}).pipe(
  Command.withDescription("Register a dataset definition or manifest"),
  Command.withHandler(
    Effect.fn(function*() {
      const manifest = yield* ManifestContext.ManifestContext
      const registry = yield* Registry.Registry
      const result = yield* registry.register(manifest)
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromFile({ manifest: args.manifest, config: args.config }).pipe(
      Layer.provideMerge(Registry.layer(`${args.registryUrl}`)),
    )
  ),
)
