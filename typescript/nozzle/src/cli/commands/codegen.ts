import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Registry from "../../api/Registry.ts"
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
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const generator = yield* SchemaGenerator.SchemaGenerator
      if (Option.isSome(args.query)) {
        const result = yield* generator.fromSql(args.query.value)
        return yield* Console.log(result)
      }

      const manifest = yield* Effect.serviceOptional(ManifestContext.ManifestContext).pipe(Effect.orDie)
      yield* Console.log(generator.fromManifest(manifest))
    }),
  ),
  Command.provide(({ args }) =>
    Option.match(args.query, {
      onSome: () => Layer.empty,
      onNone: () => ManifestContext.layerFromFile({ config: args.config, manifest: args.manifest }),
    }).pipe(Layer.merge(SchemaGenerator.SchemaGenerator.Default), Layer.provide(Registry.layer(`${args.registry}`)))
  ),
)
