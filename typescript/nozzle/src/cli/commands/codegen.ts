import { Command, Options } from "@effect/cli";
import { Console, Effect, Option, Unify } from "effect";
import * as ManifestBuilder from "../../ManifestBuilder.js";
import * as SchemaGenerator from "../../SchemaGenerator.js";
import * as ConfigLoader from "../../ConfigLoader.js";
import * as ManifestLoader from "../../ManifestLoader.js";

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
  }
}, ({ args }) => Effect.gen(function* () {
  const generator = yield* SchemaGenerator.SchemaGenerator;
  if (Option.isSome(args.query)) {
    const result = yield* generator.fromSql(args.query.value).pipe(Effect.orDie);
    yield* Console.log(result);
    return;
  }

  const loader = yield* ManifestLoader.ManifestLoader;
  const config = yield* ConfigLoader.ConfigLoader;
  const builder = yield* ManifestBuilder.ManifestBuilder;
  const manifest = yield* Unify.unify(Option.match(args.manifest, {
    onSome: (file) => loader.load(file).pipe(Effect.map(Option.some)),
    onNone: () => Unify.unify(Option.match(args.config, {
      onSome: (file) => config.load(file).pipe(Effect.flatMap(builder.build), Effect.map(Option.some)),
      onNone: () => config.find().pipe(Effect.flatMap(Unify.unify(Option.match({
        onSome: (definition) => builder.build(definition).pipe(Effect.map(Option.some)),
        onNone: () => loader.load("nozzle.json").pipe(Effect.map(Option.some)),
      })))),
    }))
  })).pipe(
    Effect.orDie,
    Effect.flatMap(Option.match({
      onNone: () => Effect.die("No manifest or config file provided"),
      onSome: Effect.succeed,
    })),
  );

  const result = yield* generator.fromManifest(manifest).pipe(Effect.orDie);
  yield* Console.log(result);
})).pipe(
  Command.withDescription("Generate schema definition code for a dataset"),
  Command.provide(SchemaGenerator.SchemaGenerator.Default),
  Command.provide(ManifestBuilder.ManifestBuilder.Default),
  Command.provide(ManifestLoader.ManifestLoader.Default),
  Command.provide(ConfigLoader.ConfigLoader.Default),
);
