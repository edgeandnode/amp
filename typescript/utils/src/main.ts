#!/usr/bin/env bun

import { Command } from "@effect/cli"
import { PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Effect, Layer } from "effect"

import { build } from "./commands/build.js"

const nozzle = Command.make("nozzle-utils").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build])
)

const cli = Command.run(nozzle, {
  name: "Nozzle Utils",
  version: "v0.0.1"
})

const layer = Layer.provideMerge(
  PlatformConfigProvider.layerDotEnvAdd(".env"),
  NodeContext.layer
)

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(layer)
)

runnable.pipe(NodeRuntime.runMain)
