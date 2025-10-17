import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import { adminUrl } from "../common.ts"

const list = Command.make("list", {
  args: {
    adminUrl,
  },
}).pipe(
  Command.withDescription("List all datasets"),
  Command.withHandler(
    Effect.fn(function*(_args) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getDatasets()

      if (response.datasets.length === 0) {
        return yield* Console.log("No datasets found")
      }

      yield* Console.table(response.datasets.map((dataset) => ({
        Name: dataset.name,
        Version: dataset.version,
      })))
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

const show = Command.make("show", {
  args: {
    name: Args.text({ name: "name" }).pipe(
      Args.withDescription("The dataset name to show details for"),
    ),
    version: Options.text("version").pipe(
      Options.withAlias("v"),
      Options.withDescription("The dataset version (if not specified, shows the latest)"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Show detailed information about a dataset"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const dataset = yield* (args.version.pipe(Option.isSome)
        ? admin.getDatasetVersion(args.name, args.version.pipe(Option.getOrThrow))
        : admin.getDataset(args.name)).pipe(
          Effect.catchTags({
            DatasetNotFound: () =>
              Effect.dieMessage(
                `Dataset not found: ${args.name}${
                  args.version.pipe(Option.isSome) ? `@${args.version.pipe(Option.getOrThrow)}` : ""
                }`,
              ),
            InvalidSelector: () => Effect.dieMessage(`Invalid dataset name: ${args.name}`),
          }),
        )

      yield* Console.log(`Dataset: ${dataset.name}`)
      yield* Console.log(`Kind: ${dataset.kind}`)
      yield* Console.log(`\nTables:`)

      if (dataset.tables.length === 0) {
        yield* Console.log("  No tables found")
      } else {
        yield* Console.table(dataset.tables.map((table) => ({
          Name: table.name,
          Network: table.network,
          Location: table.activeLocation ?? "N/A",
        })))
      }
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

const versions = Command.make("versions", {
  args: {
    name: Args.text({ name: "name" }).pipe(
      Args.withDescription("The dataset name to list versions for"),
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("List all versions of a specific dataset"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getDatasetVersions(args.name).pipe(
        Effect.catchTags({
          InvalidSelector: () => Effect.dieMessage(`Invalid dataset name: ${args.name}`),
        }),
      )

      if (response.versions.length === 0) {
        return yield* Console.log(`No versions found for dataset: ${args.name}`)
      }

      yield* Console.log(`Versions for dataset: ${args.name}\n`)

      for (const version of response.versions) {
        yield* Console.log(`  ${version}`)
      }
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

export const datasets = Command.make("datasets").pipe(
  Command.withDescription("Explore and manage datasets"),
  Command.withSubcommands([list, show, versions]),
)
