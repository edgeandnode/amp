#!/usr/bin/env node

import { Effect } from "effect";
import { Command } from "@effect/cli";
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

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
);

runnable.pipe(NodeRuntime.runMain);
