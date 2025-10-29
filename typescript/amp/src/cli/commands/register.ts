import * as Args from "@effect/cli/Args"
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
    tag: Args.text({ name: "tag" }).pipe(
      Args.withDescription("Dataset version (semver) or 'dev' tag"),
      Args.withSchema(Schema.Union(Model.DatasetVersion, Model.DatasetDevTag)),
      Args.optional,
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

      // If tag is not provided or is "dev", register without version tag (bumps dev tag)
      // Otherwise, register with the specified semantic version
      const version = Option.match(args.tag, {
        onNone: () => Option.none(),
        onSome: (tag) => (Schema.is(Model.DatasetDevTag)(tag) ? Option.none() : Option.some(tag)),
      })

      const result = yield* client.registerDataset(
        context.metadata.namespace,
        context.metadata.name,
        version,
        context.manifest,
      )
      yield* Console.log(result)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile)
      .pipe(
        Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
      )
  ),
)
