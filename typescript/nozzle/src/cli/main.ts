#!/usr/bin/env bun

import { Command, Options, ValidationError } from "@effect/cli"
import { PlatformConfigProvider } from "@effect/platform"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Config, Effect, Layer, Logger, LogLevel, String } from "effect"

import { build } from "./commands/build.js"
import { codegen } from "./commands/codegen.js"
import { deploy } from "./commands/deploy.js"
import { dev } from "./commands/dev.js"
import { proxy } from "./commands/proxy.js"
import { query } from "./commands/query.js"

const levels = LogLevel.allLevels.map((value) => String.toLowerCase(value.label)) as Array<Lowercase<LogLevel.Literal>>
const nozzle = Command.make("nozzle", {
  args: {
    logs: Options.choice("log-level", levels).pipe(
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
  Effect.catchIf(ValidationError.isValidationError, () => Effect.void),
  Effect.catchTags({
    ArrowFlightError: (cause) => Effect.logError(cause.message),
    ManifestBuilderError: (cause) => Effect.logError(cause.message),
    ManifestDeployerError: (cause) => Effect.logError(cause.message),
    ConfigLoaderError: (cause) => Effect.logError(cause.message),
    ManifestLoaderError: (cause) => Effect.logError(cause.message),
    SchemaGeneratorError: (cause) => Effect.logError(cause.message),
  }),
)

runnable.pipe(NodeRuntime.runMain)
