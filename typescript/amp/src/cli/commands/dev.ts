import * as Command from "@effect/cli/Command"
import { Effect } from "effect"
import * as Console from "effect/Console"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ConfigLoader from "../../ConfigLoader.ts"
import * as Model from "../../Model.ts"
import { adminUrl } from "../common.ts"

export const dev = Command.make("dev", { args: { adminUrl } }).pipe(
  Command.withDescription("Run a development server with hot reloading"),
  Command.withHandler(
    Effect.fn(function*() {
      const admin = yield* Admin.Admin
      const configLoader = yield* ConfigLoader.ConfigLoader

      // Find the amp.config.ts file in current directory
      const configFile = yield* configLoader.find().pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.dieMessage("Could not find amp.config.ts file in current directory"),
            onSome: (configFile) => Effect.succeed(configFile),
          }),
        ),
      )

      // Watch config file for changes
      yield* Console.info(`Watching ${configFile} for changes`)
      const configChanges = configLoader.watch(configFile, {
        onError: (cause) => Console.error("Invalid dataset configuration", cause),
      })

      // Register and deploy on each change
      yield* Stream.runForEach(configChanges, ({ manifest, metadata }) =>
        Effect.gen(function*() {
          // Register and deploy dataset under `dev` revision
          yield* Console.info(`Config changed, deploying ${metadata.namespace}/${metadata.name}@dev`)
          yield* admin.registerDataset(metadata.namespace, metadata.name, manifest)
          yield* admin.deployDataset(metadata.namespace, metadata.name, Model.DatasetTag.make("dev"))
        }).pipe(
          Effect.tapError((cause) => Console.error(`Failed to deploy ${metadata.namespace}/${metadata.name}`, cause)),
          Effect.ignore,
        ))
    }),
  ),
  Command.provide(({ args }) =>
    ConfigLoader.ConfigLoader.Default.pipe(
      Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
      Layer.provide(Auth.layer),
    )
  ),
)
