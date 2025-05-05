#!/usr/bin/env bun

import { Command, Options, ValidationError } from "@effect/cli"
import { Error as PlatformError, PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Cause, Config, Console, Effect, Layer, Logger, LogLevel, String } from "effect"
import * as Utils from "../Utils.js"

import { build } from "./commands/build.js"
import { codegen } from "./commands/codegen.js"
import { deploy } from "./commands/deploy.js"
import { dev } from "./commands/dev.js"
import { proxy } from "./commands/proxy.js"
import { query } from "./commands/query.js"

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

    // TODO: This is a temporary hack to handle @effect/platform errors whilst those are not using `Data.TaggedError`.
    if (PlatformError.isPlatformError(squashed)) {
      const error = new Error(squashed.message)
      error.cause = (squashed as any).cause
      return Console.error(Utils.prettyCause(Cause.fail(error)))
    }

    return Console.error(Utils.prettyCause(cause))
  }),
)

runnable.pipe(NodeRuntime.runMain({ disableErrorReporting: true }))
