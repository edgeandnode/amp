import { Args, Command, Options } from "@effect/cli";
import { Path, FileSystem } from "@effect/platform";
import { Console, Effect, Match, Option, Predicate, Schema } from "effect";
import { ManifestBuilder, Model, SchemaGenerator } from "@nozzle/nozzle";
import { importFile, readJson } from "../common.js";

export const codegen = Command.make("codegen", {
  args: {
    input: Args.text({ name: "file" }).pipe(
      Args.withDescription("The dataset definition or manifest file to generate code for"),
      Args.optional,
    ),
    query: Options.text("query").pipe(
      Options.withDescription("The query to generate code for"),
      Options.optional,
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const generator = yield* SchemaGenerator.SchemaGenerator;
  const builder = yield* ManifestBuilder.ManifestBuilder;

  if (Option.isSome(args.query)) {
    const result = yield* generator.fromSql(args.query.value);
    yield* Console.log(result);
    return;
  }

  const file = yield* Option.match(args.input, {
    onNone: () => {
      const candidates = [
        path.resolve("nozzle.config.ts"),
        path.resolve("nozzle.config.js"),
        path.resolve("nozzle.json"),
      ]

      const effects = candidates.map((path) => fs.exists(path).pipe(Effect.filterOrFail(Predicate.isTruthy), Effect.as(path)))
      return Effect.firstSuccessOf(effects).pipe(Effect.option);
    },
    onSome: (dataset) => Effect.succeed(Option.some(path.resolve(dataset))),
  }).pipe(Effect.flatten, Effect.mapError((e) => new Error(`Failed to find dataset definition file`, { cause: e })));

  const manifest = yield* Match.value(path.extname(file)).pipe(
    Match.when(Match.is(".json"), () => readJson(file).pipe(
      Effect.flatMap(Schema.decodeUnknown(Model.DatasetManifest)),
    )),
    Match.when(Match.is(".js"), () => importFile(file).pipe(
      Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
      Effect.flatMap(builder.build),
    )),
    Match.when(Match.is(".ts"), () => importFile(file).pipe(
      Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)),
      Effect.flatMap(builder.build),
    )),
    Match.orElse(() => Effect.fail(new Error(`Expected a manifest (.json) or a dataset definition file (.js or .ts)`)))
  ).pipe(Effect.mapError((e) => new Error(`Failed to load manifest from ${file}`, { cause: e })))

  const result = yield* generator.fromManifest(manifest);
  yield* Console.log(result);
})).pipe(
  Command.withDescription("Deploy a dataset to Nozzle"),
  Command.provide(ManifestBuilder.ManifestBuilder.Default),
  Command.provide(SchemaGenerator.SchemaGenerator.Default),
);
