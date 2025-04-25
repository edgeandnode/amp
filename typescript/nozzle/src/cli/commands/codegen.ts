import { Command, Options } from "@effect/cli"
import { Config, Console, Effect, Layer, Option, Unify } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as ManifestLoader from "../../ManifestLoader.js"
import * as SchemaGenerator from "../../SchemaGenerator.js"

export const codegen = Command.make("codegen", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to generate code for"),
    ),
    manifest: Options.text("manifest").pipe(
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
    ),
  },
}, ({ args }) => {
  const command = Effect.gen(function*() {
    const generator = yield* SchemaGenerator.SchemaGenerator
    if (Option.isSome(args.query)) {
      const result = yield* generator.fromSql(args.query.value)
      return yield* Console.log(result)
    }

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
      Effect.flatMap(Option.match({
        onNone: () => Effect.dieMessage("No manifest or config file provided"),
        onSome: Effect.succeed,
      })),
    )

    const result = yield* generator.fromManifest(manifest)
    yield* Console.log(result)
  })

  const layer = Layer.mergeAll(
    SchemaGenerator.SchemaGenerator.Default,
    ManifestBuilder.ManifestBuilder.Default,
    ManifestLoader.ManifestLoader.Default,
    ConfigLoader.ConfigLoader.Default,
  ).pipe(Layer.provide(Api.Registry.withUrl(args.registry)))

  return command.pipe(Effect.provide(layer))
}).pipe(
  Command.withDescription("Generate schema definition code for a dataset"),
)
