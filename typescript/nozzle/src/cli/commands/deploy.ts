import { Args, Command } from "@effect/cli";
import { Path, FileSystem } from "@effect/platform";
import { Effect, Match, Option, Predicate, Schema } from "effect";
import { importFile, readJson } from "../common.js";
import * as ManifestBuilder from "../../ManifestBuilder.js";
import * as ManifestDeployer from "../../ManifestDeployer.js";
import * as Model from "../../Model.js";

export const deploy = Command.make("deploy", {
  args: {
    input: Args.text({ name: "file" }).pipe(
      Args.withDescription("The dataset definition or manifest file to deploy"),
      Args.optional,
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const deployer = yield* ManifestDeployer.ManifestDeployer;
  const builder = yield* ManifestBuilder.ManifestBuilder;
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

  const result = yield* deployer.deploy(manifest);
  yield* Effect.log(result);
})).pipe(
  Command.withDescription("Deploy a dataset definition or manifest to Nozzle"),
  Command.provide(ManifestDeployer.ManifestDeployer.Default),
  Command.provide(ManifestBuilder.ManifestBuilder.Default),
);
