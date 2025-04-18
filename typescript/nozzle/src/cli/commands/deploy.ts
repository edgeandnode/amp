import { Command, Options } from "@effect/cli";
import { Effect, Option, Unify } from "effect";
import * as ManifestBuilder from "../../ManifestBuilder.js";
import * as ConfigLoader from "../../ConfigLoader.js";
import * as ManifestLoader from "../../ManifestLoader.js";
import * as ManifestDeployer from "../../ManifestDeployer.js";

export const deploy = Command.make("deploy", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to deploy"),
    ),
    manifest: Options.text("manifest").pipe(
      Options.optional,
      Options.withAlias("m"),
      Options.withDescription("The dataset manifest file to deploy"),
    ),
  }
}, ({ args }) => Effect.gen(function* () {
  const deployer = yield* ManifestDeployer.ManifestDeployer;
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
      onNone: () => Effect.dieMessage("No manifest or config file provided"),
      onSome: Effect.succeed,
    })),
  );

  const result = yield* deployer.deploy(manifest);
  yield* Effect.log(result);
})).pipe(
  Command.withDescription("Deploy a dataset definition or manifest to Nozzle"),
  Command.provide(ManifestDeployer.ManifestDeployer.Default),
  Command.provide(ManifestBuilder.ManifestBuilder.Default),
  Command.provide(ManifestLoader.ManifestLoader.Default),
  Command.provide(ConfigLoader.ConfigLoader.Default),
);
