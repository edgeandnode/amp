import { Args, Command, Options } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Effect, Schema, Console } from "effect";
import { ManifestBuilder } from "./ManifestBuilder.js";
import { ManifestDeployer } from "./ManifestDeployer.js";
import { Path, FileSystem } from "@effect/platform";
import * as Model from "./Model.js";

const build = Command.make("build", {
  args: {
    output: Options.text("output").pipe(
      Options.withDescription("The output file to write the manifest to")
    ),
    dataset: Args.text({ name: "Dataset definition file" }).pipe(
      Args.withDescription("The dataset definition file to build")
    ),
  },
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const builder = yield* ManifestBuilder;
  const dataset = yield* Effect.tryPromise({
    try: () => import(path.resolve(args.dataset)).then((m) => m.default),
    catch: () => new Error(`Failed to load dataset definition`),
  });

  const parsed = yield* Schema.decodeUnknown(Model.DatasetDefinition)(dataset);
  const manifest = yield* builder.build(parsed);
  const encoded = yield* Schema.encode(Model.DatasetManifest)(manifest);
  const json = JSON.stringify(encoded, null, 2);
  yield* fs.writeFileString(path.resolve(args.output), json);
})).pipe(
  Command.provide(ManifestBuilder.Default),
  Command.withDescription("Build a dataset")
);

const deploy = Command.make("deploy", {
  args: {
    dataset: Args.text({ name: "Dataset definition file" }).pipe(
      Args.withDescription("The dataset definition file to deploy")
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const path = yield* Path.Path;
  const deployer = yield* ManifestDeployer;
  const dataset = yield* Effect.tryPromise({
    try: () => import(path.resolve(args.dataset)).then((m) => m.default),
    catch: () => new Error(`Failed to load dataset definition`),
  });

  const parsed = yield* Schema.decodeUnknown(Model.DatasetDefinition)(dataset);
  const result = yield* deployer.deploy(parsed);
  yield* Effect.log(result);
})).pipe(
  Command.provide(ManifestDeployer.Default),
  Command.withDescription("Deploy a dataset to Nozzle")
);

const command = Command.make("nozzle").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build, deploy]),
);

const cli = Command.run(command, {
  name: "Nozzle",
  version: "v0.0.1",
});

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
);

runnable.pipe(NodeRuntime.runMain);
