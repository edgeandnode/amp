#!/usr/bin/env node

import { Command, Options, ValidationError } from "@effect/cli"
import { PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Cause, Config, Console, Effect, Layer, Logger, LogLevel, String } from "effect"
import * as Utils from "../Utils.ts"

import { build } from "./commands/build.ts"
import { codegen } from "./commands/codegen.ts"
import { deploy } from "./commands/deploy.ts"
import { dev } from "./commands/dev.ts"
import { proxy } from "./commands/proxy.ts"
import { query } from "./commands/query.ts"

const levels = LogLevel.allLevels.map((value) => String.toLowerCase(value.label)) as Array<Lowercase<LogLevel.Literal>>
const nozzle = Command.make("nozzle", {
  args: {
    logs: Options.choice("logs", levels).pipe(
      Options.withFallbackConfig(Config.string("NOZZLE_LOG_LEVEL").pipe(Config.withDefault("info"))),
      Options.withDescription("The log level to use"),
      Options.map((value) => LogLevel.fromLiteral(String.capitalize(value) as LogLevel.Literal)),
    ),
  },
}).pipe(
  Command.withDescription("The Nozzle Command Line Interface"),
  Command.withSubcommands([build, deploy, dev, codegen, query, proxy]),
  Command.provide(({ args }) => Logger.minimumLogLevel(args.logs)),
)

const cli = Command.run(nozzle, {
  name: "Nozzle",
  version: "v0.0.1",
})

const layer = Layer.provideMerge(
  PlatformConfigProvider.layerDotEnvAdd(".env"),
  NodeContext.layer,
)

const runnable = Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(layer),
  Effect.tapErrorCause((cause) => {
    const squashed = Cause.squash(cause)
    // Command validation errors are already printed by @effect/cli.
    if (ValidationError.isValidationError(squashed)) {
      return Effect.void
    }

    return Console.error(Utils.prettyCause(cause))
  }),
)

runnable.pipe(NodeRuntime.runMain({ disableErrorReporting: true }))
