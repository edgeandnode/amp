import { Console, Effect, Match, Option, Predicate, Schema } from "effect";
import { Args, Command, Options } from "@effect/cli";
import { Path, FileSystem } from "@effect/platform";
import { ManifestBuilder, Model } from "@nozzle/nozzle";
import { importFile } from "../common.js";

export const build = Command.make("build", {
  args: {
    output: Options.text("output").pipe(
      Options.optional,
      Options.withDescription("The output file to write the manifest to")
    ),
    input: Args.text({ name: "file" }).pipe(
      Args.optional,
      Args.withDescription("The dataset definition file to build")
    ),
  },
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const builder = yield* ManifestBuilder.ManifestBuilder;
  const file = yield* Option.match(args.input, {
    onNone: () => {
      const candidates = [
        path.resolve("nozzle.config.ts"),
        path.resolve("nozzle.config.js"),
      ]

      const effects = candidates.map((path) => fs.exists(path).pipe(Effect.filterOrFail(Predicate.isTruthy), Effect.as(path)))
      return Effect.firstSuccessOf(effects).pipe(Effect.option);
    },
    onSome: (dataset) => Effect.succeed(Option.some(path.resolve(dataset))),
  }).pipe(Effect.flatten, Effect.mapError((e) => new Error(`Failed to find dataset definition file`, { cause: e })));

  const dataset = yield* Match.value(path.extname(file)).pipe(
    Match.when(Match.is(".js"), () => importFile(file)),
    Match.when(Match.is(".ts"), () => importFile(file)),
    Match.orElse(() => Effect.fail(new Error(`Expected a dataset definition file (.js or .ts).`)))
  ).pipe(Effect.flatMap(Schema.decodeUnknown(Model.DatasetDefinition)))

  const json = yield* builder.build(dataset).pipe(
    Effect.flatMap(Schema.encode(Model.DatasetManifest)),
    Effect.map((manifest) => JSON.stringify(manifest, null, 2)),
  );

  yield* Option.match(args.output, {
    onNone: () => Console.log(json),
    onSome: (output) => fs.writeFileString(path.resolve(output), json).pipe(
      Effect.tap(() => Effect.log(`Manifest written to ${output}`)),
    ),
  });
})).pipe(
  Command.withDescription("Build a manifest from a dataset definition"),
  Command.provide(ManifestBuilder.ManifestBuilder.Default),
);
