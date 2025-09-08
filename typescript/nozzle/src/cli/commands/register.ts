import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Registry from "../../api/Registry.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import { configFile, manifestFile, registryUrl } from "../common.ts"

export const register = Command.make("register", {
  args: {
    configFile: configFile.pipe(Options.optional),
    manifestFile: manifestFile.pipe(Options.optional),
    registryUrl,
  },
}).pipe(
  Command.withDescription("Register a dataset definition or manifest"),
  Command.withHandler(
    Effect.fn(function*() {
      const manifest = yield* ManifestContext.ManifestContext
      const registry = yield* Registry.Registry
      const result = yield* registry.register(manifest)
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromFile({ manifest: args.manifestFile, config: args.configFile }).pipe(
      Layer.provideMerge(Registry.layer(`${args.registryUrl}`)),
    )
  ),
)
