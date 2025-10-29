import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

export const deploy = Command.make("deploy", {
  args: {
    reference: Args.text({ name: "reference" }).pipe(
      Args.withDescription("The dataset reference to deploy (<namespace>/<name>@<revision>)"),
      Args.withSchema(Model.DatasetReferenceStr),
      Args.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    endBlock: Options.text("end-block").pipe(
      Options.withAlias("e"),
      Options.withDescription("The block number to end at, inclusive"),
      Options.optional,
    ),
    parallelism: Options.integer("parallelism").pipe(
      Options.withAlias("j"),
      Options.withDescription("Number of parallel workers for extraction"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Deploy a dataset version for extraction and transformation"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin

      // Determine dataset name and revision
      let namespace: Model.DatasetNamespace
      let name: Model.DatasetName
      let revision: Model.DatasetRevision

      if (Option.isSome(args.reference)) {
        // Parse the reference to extract name and revision
        const ref = yield* Model.parseDatasetReference(args.reference.value)
        namespace = ref.namespace
        name = ref.name
        revision = ref.revision
      } else {
        // Get dataset from ManifestContext and use "dev" tag
        const context = yield* ManifestContext.ManifestContext
        namespace = context.metadata.namespace
        name = context.metadata.name
        revision = "dev" as const
      }

      // Deploy the dataset
      yield* admin.deployDataset(namespace, name, revision, {
        endBlock: Option.getOrUndefined(args.endBlock),
        parallelism: Option.getOrUndefined(args.parallelism),
      })

      yield* Console.log(`Deployment started for dataset ${namespace}/${name}@${revision}`)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(
      args.configFile,
    ).pipe(
      Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
    )
  ),
)
