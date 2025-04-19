#!/usr/bin/env node

import { Command } from "@effect/cli"
import { PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Effect, Layer } from "effect"

import { build } from "./commands/build.js"
import { codegen } from "./commands/codegen.js"
import { deploy } from "./commands/deploy.js"
import { proxy } from "./commands/proxy.js"
import { query } from "./commands/query.js"

const nozzle = Command.make("nozzle").pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build, deploy, codegen, query, proxy])
)

const cli = Command.run(nozzle, {
  name: "Nozzle",
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
