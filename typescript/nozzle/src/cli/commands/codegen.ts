import { Command, Options } from "@effect/cli"
import { Config, Console, Effect, Layer, Option, Schema } from "effect"
import * as Api from "../../Api.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as SchemaGenerator from "../../SchemaGenerator.ts"

export const codegen = Command.make("codegen", {
  args: {
    config: Options.file("config", { exists: "yes" }).pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to generate code for"),
    ),
    manifest: Options.file("manifest", { exists: "yes" }).pipe(
      Options.optional,
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to generate code for"),
    ),
    query: Options.text("query").pipe(
      Options.optional,
      Options.withAlias("q"),
      Options.withDescription("The query to generate code for"),
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
  Command.withDescription("Generate schema definition code for a dataset"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const generator = yield* SchemaGenerator.SchemaGenerator
      if (Option.isSome(args.query)) {
        const result = yield* generator.fromSql(args.query.value)
        return yield* Console.log(result)
      }

      const manifest = yield* Effect.serviceOptional(ManifestContext.ManifestContext).pipe(Effect.orDie)
      const result = yield* generator.fromManifest(manifest)
      yield* Console.log(result)
    })
  ),
  Command.provide(({ args }) =>
    Option.match(args.query, {
      onSome: () => Layer.empty,
      onNone: () => ManifestContext.layerFromFile({ config: args.config, manifest: args.manifest }),
    }).pipe(
      Layer.merge(SchemaGenerator.SchemaGenerator.Default),
      Layer.provide(Api.Registry.withUrl(`${args.registry}`)),
    )
  ),
)
