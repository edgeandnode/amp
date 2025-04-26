#!/usr/bin/env bun

import { Command, Options, ValidationError } from "@effect/cli"
import { PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Cause, Config, Console, Effect, Layer, Logger, LogLevel, String } from "effect"

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
    if (ValidationError.isValidationError(Cause.squash(cause))) {
      return Effect.void
    }

    return Console.error(prettyCause(cause))
  }),
)

runnable.pipe(NodeRuntime.runMain({ disableErrorReporting: true }))

const prettyCause = <E>(cause: Cause.Cause<E>): string => {
  if (Cause.isInterruptedOnly(cause)) {
    return "All fibers interrupted without errors."
  }

  return Cause.prettyErrors<E>(cause).map((error) => {
    const output = (error.stack ?? "").split("\n")[0] ?? ""
    return error.cause ? `${output}\n\n${renderCause(error.cause as Cause.PrettyError, "└──")}` : output
  }).join("\n\n")
}

const renderCause = (cause: Cause.PrettyError, prefix: string): string => {
  const output = `${prefix}[cause]: ${(cause.stack ?? "").split("\n")[0] ?? ""}`
  return cause.cause ? `${output}\n\n${renderCause(cause.cause as Cause.PrettyError, `${prefix}──`)}` : output
}
