import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as Model from "../../Model.ts"
import { adminUrl, force } from "../common.ts"

const list = Command.make("list", {
  args: {
    limit: Options.integer("limit").pipe(
      Options.withAlias("l"),
      Options.withDescription("Maximum number of jobs to return (default: 50, max: 1000)"),
      Options.withDefault(50),
      Options.optional,
    ),
    after: Options.integer("after").pipe(
      Options.withAlias("a"),
      Options.withDescription("Job identifier to paginate after"),
      Options.optional,
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("List jobs with pagination"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const response = yield* admin.getJobs({
        limit: args.limit.pipe(Option.getOrUndefined),
        lastJobId: args.after.pipe(Option.getOrUndefined),
      })

      if (response.jobs.length === 0) {
        return yield* Console.log("No jobs found")
      }

      // Format jobs for table display
      yield* Console.table(response.jobs.map((job) => ({
        Id: job.id,
        Status: job.status,
        Created: DateTime.formatIso(job.createdAt),
        Updated: DateTime.formatIso(job.updatedAt),
        Node: job.nodeId,
        Descriptor: job.descriptor,
      })))
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

// Jobs show subcommand
const show = Command.make("show", {
  args: {
    id: Args.integer({ name: "id" }).pipe(
      Args.withDescription("The job identifier to show details for"),
    ),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Show detailed information about a job"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const job = yield* admin.getJobById(args.id).pipe(
        Effect.catchTags({
          JobNotFound: () => Effect.dieMessage(`Job not found: ${args.id}`),
          InvalidJobId: () => Effect.dieMessage(`Invalid job identifier: ${args.id}`),
        }),
      )

      yield* Console.log(`Job Details:`)
      yield* Console.log(`  Id: ${job.id}`)
      yield* Console.log(`  Status: ${job.status}`)
      yield* Console.log(`  Created: ${DateTime.formatIso(job.createdAt)}`)
      yield* Console.log(`  Updated: ${DateTime.formatIso(job.updatedAt)}`)
      yield* Console.log(`  Node: ${job.nodeId}`)
      yield* Console.log(`  Descriptor: ${JSON.stringify(job.descriptor)}`)
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

// Jobs stop subcommand
const stop = Command.make("stop", {
  args: {
    id: Args.integer({ name: "id" }).pipe(
      Args.withDescription("The job ID to stop"),
    ),
    force,
    adminUrl,
  },
}).pipe(
  Command.withDescription("Stop a running job"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      const job = yield* admin.getJobById(args.id).pipe(
        Effect.catchTags({
          JobNotFound: () => Effect.dieMessage(`Job not found: ${args.id}`),
          InvalidJobId: () => Effect.dieMessage(`Invalid job identifier: ${args.id}`),
        }),
      )

      if (!args.force) {
        const confirmed = yield* Prompt.confirm({
          message: `Stop job ${job.id} (status: ${job.status})?`,
          initial: false,
        })

        if (!confirmed) {
          return yield* Console.log("Operation cancelled")
        }
      }

      yield* admin.stopJob(args.id).pipe(
        Effect.catchTags({
          JobConflict: () => Effect.dieMessage(`Job cannot be stopped: ${args.id}`),
          JobNotFound: () => Effect.dieMessage(`Job not found: ${args.id}`),
          InvalidJobId: () => Effect.dieMessage(`Invalid job identifier: ${args.id}`),
        }),
      )

      yield* Console.log(`Job ${args.id} stop requested`)
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

// Jobs delete subcommand
const del = Command.make("delete", {
  args: {
    target: Args.text({ name: "target" }).pipe(
      Args.withDescription("Job identifier or status filter"),
      Args.withSchema(Schema.Union(Model.JobStatusParam, Model.JobIdParam)),
    ),
    force,
    adminUrl,
  },
}).pipe(
  Command.withDescription("Delete job(s) by identifier or status filter"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const admin = yield* Admin.Admin
      if (Schema.is(Model.JobIdParam)(args.target)) {
        const job = yield* admin.getJobById(args.target).pipe(
          Effect.catchTags({
            JobNotFound: () => Effect.dieMessage(`Job not found: ${args.target}`),
            InvalidJobId: () => Effect.dieMessage(`Invalid job identifier: ${args.target}`),
          }),
        )

        if (!args.force) {
          const confirmed = yield* Prompt.confirm({
            message: `Delete job ${job.id} (status: ${job.status})?`,
            initial: false,
          })

          if (!confirmed) {
            return yield* Console.log("Operation cancelled")
          }
        }

        yield* admin.deleteJobById(job.id).pipe(
          Effect.catchTags({
            JobConflict: () => Effect.dieMessage(`Job cannot be deleted: ${args.target}`),
            JobNotFound: () => Effect.dieMessage(`Job not found: ${args.target}`),
            InvalidJobId: () => Effect.dieMessage(`Invalid job identifier: ${args.target}`),
          }),
        )

        return yield* Console.log(`Job ${job.id} deleted`)
      }

      if (!args.force) {
        const confirmed = yield* Prompt.confirm({
          message: `Delete all jobs with status "${args.target}"?`,
          initial: false,
        })

        if (!confirmed) {
          return yield* Console.log("Operation cancelled")
        }
      }

      yield* admin.deleteAllJobs(args.target)
      yield* Console.log(`Jobs with status "${args.target}" deleted`)
    }),
  ),
  Command.provide(({ args }) => Admin.layer(`${args.adminUrl}`)),
)

export const jobs = Command.make("jobs").pipe(
  Command.withDescription("Manage jobs"),
  Command.withSubcommands([list, show, stop, del]),
)
