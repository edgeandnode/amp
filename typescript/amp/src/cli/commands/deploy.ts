import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile, ExitCode } from "../common.ts"

const TERMINAL_STATUSES: ReadonlyArray<typeof Model.JobStatus.Type> = ["COMPLETED", "STOPPED", "ERROR", "FATAL"]

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
    force: Options.boolean("force").pipe(
      Options.withAlias("f"),
      Options.withDescription("Skip confirmation prompt when base datasets are in terminal states"),
      Options.withDefault(false),
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

      // Check base dataset statuses before deploying
      yield* checkBaseDatasetsStatus(admin, dataset, args.force)

      // Deploy the dataset
      yield* admin.deployDataset(dataset.namespace, dataset.name, dataset.revision, {
        endBlock: Option.getOrUndefined(args.endBlock),
      })

      yield* Console.log(`Deployment started for dataset ${dataset.namespace}/${dataset.name}@${dataset.revision}`)
    }),
  ),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile).pipe(Layer.provideMerge(
      Layer.unwrapEffect(Effect.gen(function*() {
        const token = yield* Auth.AuthService.pipe(Effect.flatMap((auth) => auth.getCache()))
        return Admin.layer(`${args.adminUrl}`, Option.getOrUndefined(token)?.accessToken)
      })).pipe(Layer.provide(Auth.layer)),
    ))
  ),
)

const checkBaseDatasetsStatus = Effect.fn(function*(
  admin: Context.Tag.Service<typeof Admin.Admin>,
  dataset: Model.DatasetReference,
  force: boolean,
) {
  // Fetch the manifest to check if this is a derived dataset with dependencies
  const manifest = yield* admin.getDatasetManifest(dataset.namespace, dataset.name, dataset.revision).pipe(
    Effect.catchAll(() => Effect.succeed(null)),
  )

  if (manifest === null || manifest.kind !== "manifest") {
    return
  }

  const entries = Object.entries(manifest.dependencies) as Array<[string, Model.DatasetReference]>
  if (entries.length === 0) {
    return
  }

  // Check the status of each base dataset's latest job concurrently
  const results = yield* Effect.all(
    entries.map(([alias, depRef]) =>
      admin.listDatasetJobs(depRef.namespace, depRef.name, depRef.revision).pipe(
        Effect.catchAll(() => Effect.succeed(null)),
        Effect.map((jobsResponse) => {
          if (jobsResponse === null || jobsResponse.jobs.length === 0) return null

          const latestJob = jobsResponse.jobs.reduce((latest, job) => job.id > latest.id ? job : latest)

          if (TERMINAL_STATUSES.includes(latestJob.status)) {
            return { alias, reference: depRef.encode(), status: latestJob.status }
          }
          return null
        }),
      )
    ),
    { concurrency: "unbounded" },
  )

  const terminalDeps = results.filter((r): r is NonNullable<typeof r> => r !== null)

  if (terminalDeps.length === 0) return

  // Display warning
  yield* Console.warn("Warning: The following base datasets have jobs in terminal states:")
  for (const dep of terminalDeps) {
    yield* Console.warn(`  - ${dep.alias} (${dep.reference}): ${dep.status}`)
  }

  if (force) return

  const shouldContinue = yield* Prompt.confirm({
    message: "One or more base datasets are in a terminal state. Continue deployment?",
    initial: false,
  })

  if (!shouldContinue) {
    yield* Console.log("Deployment cancelled.")
    return yield* ExitCode.Zero
  }
})
