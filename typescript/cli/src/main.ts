#!/usr/bin/env bun

import { Effect } from "effect";
import { Command } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";

import { deploy } from "./commands/deploy.js";
import { build } from "./commands/build.js";

const nozzle = Command.make("nozzle").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build, deploy]),
);

const cli = Command.run(nozzle, {
  name: "Nozzle",
  version: "v0.0.1",
});

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
);

runnable.pipe(NodeRuntime.runMain);
