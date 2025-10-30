import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Admin from "./api/Admin.ts"
import * as ConfigLoader from "./ConfigLoader.ts"

/**
 * Error type for the dev server service.
 */
export class DevServerError extends Data.TaggedError("DevServerError")<{
  readonly cause?: unknown
  readonly message: string
}> {}

/**
 * Service definition for the development server.
 */
export class DevServer extends Context.Tag("Amp/DevServer")<DevServer, void>() {}

/**
 * Creates a dev server service instance.
 */
export const make = Effect.gen(function*() {
  const admin = yield* Admin.Admin
  const configLoader = yield* ConfigLoader.ConfigLoader

  // Find the amp.config.ts file in current directory
  const configFile = yield* configLoader.find().pipe(
    Effect.flatMap(Option.match({
      onNone: () => new DevServerError({ message: "Could not find amp.config.ts file in current directory" }),
      onSome: (configFile) => Effect.succeed(configFile),
    })),
  )

  yield* Effect.logInfo(`Watching ${configFile} for changes...`)

  // Watch config file for changes
  const configChanges = configLoader.watch(configFile, {
    onError: (cause) => Effect.logError("Invalid dataset configuration", cause),
  })

  // Register and deploy on each change
  yield* Stream.runForEach(configChanges, ({ dependencies, manifest, metadata }) =>
    Effect.gen(function*() {
      yield* Effect.logInfo(`Config changed, deploying ${metadata.namespace}/${metadata.name}@dev...`)

      // Deploy dependencies first (concurrently, already parsed)
      if (dependencies.length > 0) {
        yield* Effect.logDebug(`Deploying ${dependencies.length} dependencies...`)
        yield* Effect.forEach(
          dependencies,
          (ref) => admin.deployDataset(ref.namespace, ref.name, ref.revision),
          { concurrency: "unbounded", discard: true },
        )
      }

      // Register WITHOUT version → only updates "dev" tag
      yield* admin.registerDataset(metadata.namespace, metadata.name, Option.none(), manifest)

      // Deploy using "dev" revision
      const deployResponse = yield* admin.deployDataset(metadata.namespace, metadata.name, "dev")

      yield* Effect.logInfo(
        `✅ Deployed ${metadata.namespace}/${metadata.name}@dev (job ID: ${deployResponse.jobId})`,
      )
    }).pipe(
      Effect.tapError((cause) => Effect.logError(`Failed to deploy ${metadata.namespace}/${metadata.name}`, cause)),
      Effect.ignore,
    ))
})

/**
 * Creates a dev server service layer.
 */
export const layer = () => make.pipe(Layer.scoped(DevServer))
