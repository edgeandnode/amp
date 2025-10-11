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
    limit: Options.integer("limit").pipe(
      Options.withAlias("l"),
      Options.withDescription("Maximum number of locations to return (default: 50, max: 1000)"),
      Options.withDefault(50),
      Options.optional,
    ),
    after: Options.integer("after").pipe(
      Options.withAlias("a"),
      Options.withDescription("Location identifier to paginate after"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("List locations with pagination"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getLocations({
        limit: args.limit.pipe(Option.getOrUndefined),
        lastLocationId: args.after.pipe(Option.getOrUndefined),
      })

      if (response.locations.length === 0) {
        return yield* Console.log("No locations found")
      }

      // Format locations for table display
      yield* Console.table(response.locations.map((location) => ({
        Id: location.id,
        Dataset: location.dataset,
        Version: location.datasetVersion,
        Table: location.table,
        URL: location.url,
        Active: location.active ? "Yes" : "No",
        Writer: location.writer ? location.writer.toString() : "None",
      })))
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

// Locations show subcommand
const show = Command.make("show", {
  args: {
    id: Args.integer({ name: "id" }).pipe(
      Args.withDescription("The location identifier to show details for"),
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Show detailed information about a location"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const location = yield* admin.getLocationById(args.id).pipe(
        Effect.catchTags({
          LocationNotFound: () => Effect.dieMessage(`Location not found: ${args.id}`),
          InvalidLocationId: () => Effect.dieMessage(`Invalid location identifier: ${args.id}`),
        }),
      )

      yield* Console.log(`Location Details:`)
      yield* Console.log(`  Id: ${location.id}`)
      yield* Console.log(`  Dataset: ${location.dataset}`)
      yield* Console.log(`  Version: ${location.datasetVersion}`)
      yield* Console.log(`  Table: ${location.table}`)
      yield* Console.log(`  URL: ${location.url}`)
      yield* Console.log(`  Active: ${location.active ? "Yes" : "No"}`)
      yield* Console.log(`  Writer: ${location.writer ? location.writer.toString() : "None"}`)
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

export const locations = Command.make("locations").pipe(
  Command.withDescription("Manage locations"),
  Command.withSubcommands([list, show]),
)
