import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"

export const dump = Command.make("dump", {
  args: {
    dataset: Args.text({ name: "dataset" }).pipe(Args.withDescription("The name of the dataset to dump")),
    endBlock: Options.integer("end-block").pipe(
      Options.withAlias("e"),
      Options.withDescription("The block number to end at, inclusive"),
    ),
    adminUrl: Options.text("admin-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
      Options.withDescription("The Admin API URL to use for the dump request"),
      Options.withSchema(Schema.URL),
    ),
    waitForCompletion: Options.boolean("wait").pipe(
      Options.withAlias("w"),
      Options.withDefault(false),
      Options.withDescription("Wait for the dump to complete before returning"),
    ),
  },
}).pipe(
  Command.withDescription("Dump a dataset via the Nozzle Admin API"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin

      if (args.waitForCompletion) {
        yield* Console.log(`Starting dump for dataset ${args.dataset} up to block ${args.endBlock}`)
      }

      yield* admin.dumpDataset(args.dataset, {
        endBlock: args.endBlock,
        waitForCompletion: args.waitForCompletion,
      })

      if (args.waitForCompletion) {
        yield* Console.log(`Dump completed for dataset ${args.dataset}`)
      } else {
        yield* Console.log(`Dump scheduled for dataset ${args.dataset}`)
      }
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)
