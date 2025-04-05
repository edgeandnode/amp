#!/usr/bin/env bun

import { Args, Command, Options } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Match, Option, Predicate, Schema } from "effect";
import { ManifestBuilder } from "./ManifestBuilder.js";
import { ManifestDeployer } from "./ManifestDeployer.js";
import { Path, FileSystem } from "@effect/platform";
import * as Model from "./Model.js";

const importFile = Effect.fn("importFile")((file: string) => Effect.tryPromise({
  try: () => import(file).then((m) => m.default),
  catch: () => new Error(`Failed to load dataset definition ${file}`),
}))

const readJson = Effect.fn("readJson")((file: string) => Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const json = yield* fs.readFileString(file);
  return yield* Effect.try({
    try: () => JSON.parse(json),
    catch: () => new Error(`Failed to parse JSON from ${file}`),
  })
}))

const build = Command.make("build", {
  args: {
    output: Options.text("output").pipe(
      Options.optional,
      Options.withDescription("The output file to write the manifest to")
    ),
    dataset: Args.text({ name: "Dataset definition file" }).pipe(
      Args.optional,
      Args.withDescription("The dataset definition file to build")
    ),
  },
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const builder = yield* ManifestBuilder;
  const file = yield* Option.match(args.dataset, {
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
})).pipe(Command.withDescription("Build a dataset"));

const deploy = Command.make("deploy", {
  args: {
    dataset: Args.text({ name: "Dataset definition file" }).pipe(
      Args.withDescription("The dataset definition file to deploy"),
      Args.optional,
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const deployer = yield* ManifestDeployer;
  const builder = yield* ManifestBuilder;
  const file = yield* Option.match(args.dataset, {
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

  const dataset = yield* Match.value(path.extname(file)).pipe(
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

  const result = yield* deployer.deploy(dataset);
  yield* Effect.log(result);
})).pipe(Command.withDescription("Deploy a dataset to Nozzle"));

const command = Command.make("nozzle").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([
    build.pipe(
      Command.provide(ManifestBuilder.Default),
    ),
    deploy.pipe(
      Command.provide(ManifestDeployer.Default),
      Command.provide(ManifestBuilder.Default),
    )
  ]),
);

const cli = Command.run(command, {
  name: "Nozzle",
  version: "v0.0.1",
});

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
);

runnable.pipe(NodeRuntime.runMain);
