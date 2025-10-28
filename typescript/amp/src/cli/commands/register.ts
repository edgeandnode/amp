import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import { adminUrl, configFile, datasetReference, manifestFile, parseReferenceToMetadata } from "../common.ts"

export const register = Command.make("register", {
  args: {
    reference: datasetReference.pipe(Options.optional),
    configFile: configFile.pipe(Options.optional),
    manifestFile: manifestFile.pipe(Options.optional),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Register a dataset definition or manifest"),
  Command.withHandler(
    Effect.fn(function*() {
      const context = yield* ManifestContext.ManifestContext
      const client = yield* Admin.Admin

      const result = yield* client.registerDataset(
        context.metadata.namespace,
        context.metadata.name,
        context.metadata.version,
        context.manifest,
      )
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) => {
    const metadata = Option.map(args.reference, parseReferenceToMetadata)

    return ManifestContext.layerFromFile({
      manifest: args.manifestFile,
      metadata,
      config: args.configFile,
    }).pipe(
      Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
    )
  }),
)
