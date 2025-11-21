import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

export const deploy = Command.make("deploy", {
  args: {
    reference: Options.text("reference").pipe(
      Options.withDescription("The dataset reference to deploy (<namespace>/<name>@<revision>)"),
      Options.withSchema(Model.DatasetReferenceFromString),
      Options.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    endBlock: Options.text("end-block").pipe(
      Options.withAlias("e"),
      Options.withDescription("The block number to end at, inclusive"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Deploy a dataset version for extraction and transformation"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const dataset = yield* Option.match(args.reference, {
        onSome: (dataset) => Effect.succeed(dataset),
        onNone: () =>
          ManifestContext.ManifestContext.pipe(Effect.map(({ metadata }) =>
            new Model.DatasetReference({
              name: metadata.name,
              namespace: metadata.namespace,
              revision: Model.DatasetTag.make("dev"),
            })
          )),
      })

      // Deploy the dataset
      yield* admin.deployDataset(dataset.namespace, dataset.name, dataset.revision, {
        endBlock: Option.getOrUndefined(args.endBlock),
      })

      yield* Console.log(`Deployment started for dataset ${dataset.namespace}/${dataset.name}@${dataset.revision}`)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile).pipe(
      Layer.provideMerge(Admin.layer(args.adminUrl)),
      Layer.provide(Auth.layer),
    )
  ),
)
