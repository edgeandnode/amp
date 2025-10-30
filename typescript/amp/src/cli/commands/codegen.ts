import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as SchemaGenerator from "../../SchemaGenerator.ts"
import { adminUrl, configFile } from "../common.ts"

export const codegen = Command.make("codegen", {
  args: {
    query: Args.text({ name: "query" }).pipe(
      Args.withDescription("The query to generate code for"),
      Args.optional,
    ),
    configFile: configFile.pipe(Options.optional),
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
  Command.provide(({ args }) => {
    return Option.match(args.query, {
      onSome: () => Layer.empty,
      onNone: () => ManifestContext.layerFromConfigFile(args.configFile),
    }).pipe(Layer.merge(SchemaGenerator.SchemaGenerator.Default), Layer.provide(Admin.layer(`${args.adminUrl}`)))
  }),
)
