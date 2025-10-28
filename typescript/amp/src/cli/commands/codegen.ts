import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as SchemaGenerator from "../../SchemaGenerator.ts"
import { adminUrl, configFile, manifestFile } from "../common.ts"

export const codegen = Command.make("codegen", {
  args: {
    query: Options.text("query").pipe(
      Options.withAlias("q"),
      Options.withDescription("The query to generate code for"),
      Options.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    manifestFile: manifestFile.pipe(Options.optional),
    adminUrl,
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

      const context = yield* Effect.serviceOptional(ManifestContext.ManifestContext).pipe(Effect.orDie)
      yield* Console.log(generator.fromManifest(context.manifest))
    }),
  ),
  Command.provide(({ args }) =>
    Option.match(args.query, {
      onSome: () => Layer.empty,
      onNone: () =>
        ManifestContext.layerFromFile({
          metadata: Option.none(),
          manifest: args.manifestFile,
          config: args.configFile,
        }),
    }).pipe(Layer.merge(SchemaGenerator.SchemaGenerator.Default), Layer.provide(Admin.layer(`${args.adminUrl}`)))
  ),
)
