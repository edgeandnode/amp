import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

export const register = Command.make("register", {
  args: {
    tag: Options.text("tag").pipe(
      Options.withDescription("Dataset version (semver) or 'dev' tag"),
      Options.withSchema(Schema.Union(Model.DatasetVersion, Model.DatasetDevTag)),
      Options.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Register a dataset definition from config"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const context = yield* ManifestContext.ManifestContext
      const client = yield* Admin.Admin

      const result = yield* client.registerDataset(
        context.metadata.namespace,
        context.metadata.name,
        context.manifest,
        Option.getOrUndefined(args.tag),
      )
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile).pipe(Layer.provideMerge(Admin.layer(`${args.adminUrl}`)))
  ),
)
