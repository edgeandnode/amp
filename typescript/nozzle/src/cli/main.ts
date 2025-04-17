#!/usr/bin/env node

import { Effect, Layer } from "effect";
import { Command } from "@effect/cli";
import { PlatformConfigProvider } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";

import { deploy } from "./commands/deploy.js";
import { build } from "./commands/build.js";
import { codegen } from "./commands/codegen.js";

const nozzle = Command.make("nozzle").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build, deploy, codegen]),
);

const cli = Command.run(nozzle, {
  name: "Nozzle",
  version: "v0.0.1",
});

const layer = Layer.provideMerge(
  PlatformConfigProvider.layerDotEnvAdd(".env"),
  NodeContext.layer,
);

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(layer),
);

runnable.pipe(NodeRuntime.runMain);
