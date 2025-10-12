import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Admin from "../../api/Admin.ts"
import type * as Model from "../../Model.ts"
import { adminUrl } from "../common.ts"

const list = Command.make("list", {
  args: {
    limit: Options.integer("limit").pipe(
      Options.withAlias("l"),
      Options.withDescription("Maximum number of datasets to return (default: 50, max: 1000)"),
      Options.withDefault(50),
      Options.optional,
    ),
    after: Options.text("after").pipe(
      Options.withAlias("a"),
      Options.withDescription("Dataset cursor to paginate after (format: name:version)"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("List all datasets with pagination"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getDatasets({
        limit: args.limit.pipe(Option.getOrUndefined),
        lastDatasetId: args.after.pipe(
          Option.map((cursor) => cursor as Model.DatasetCursor),
          Option.getOrUndefined,
        ),
      })

      if (response.datasets.length === 0) {
        return yield* Console.log("No datasets found")
      }

      yield* Console.table(response.datasets.map((dataset) => ({
        Name: dataset.name,
        Version: dataset.version,
        Owner: dataset.owner,
      })))

      if (response.nextCursor) {
        yield* Console.log(`\nNext cursor: ${response.nextCursor}`)
        yield* Console.log(`Use --after "${response.nextCursor}" to get the next page`)
      }
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
    limit: Options.integer("limit").pipe(
      Options.withAlias("l"),
      Options.withDescription("Maximum number of versions to return (default: 50, max: 1000)"),
      Options.withDefault(50),
      Options.optional,
    ),
    after: Options.text("after").pipe(
      Options.withAlias("a"),
      Options.withDescription("Version to paginate after (e.g., '1.0.0')"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("List all versions of a specific dataset"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getDatasetVersions(args.name, {
        limit: args.limit.pipe(Option.getOrUndefined),
        lastVersion: args.after.pipe(
          Option.map((version) => version as Model.DatasetVersionCursor),
          Option.getOrUndefined,
        ),
      }).pipe(
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

      if (response.nextCursor) {
        yield* Console.log(`\nNext cursor: ${response.nextCursor}`)
        yield* Console.log(`Use --after "${response.nextCursor}" to get the next page`)
      }
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

export const datasets = Command.make("datasets").pipe(
  Command.withDescription("Explore and manage datasets"),
  Command.withSubcommands([list, show, versions]),
)
