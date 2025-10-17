import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Admin from "./api/Admin.ts"
import * as ConfigLoader from "./ConfigLoader.ts"
import * as Model from "./Model.ts"

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

  // Find the amp.config.ts file in current directory.
  const configFile = yield* configLoader.find().pipe(
    Effect.flatMap(Option.match({
      onNone: () => new DevServerError({ message: "Could not find amp.config.ts file in current directory" }),
      onSome: (configFile) => Effect.succeed(configFile),
    })),
  )

  // Observe config changes in a sliding buffer.
  const configChanges = configLoader.watch(configFile, {
    onError: (cause) => Effect.logError("Invalid dataset configuration", cause),
  }).pipe(
    Stream.buffer({ capacity: 1, strategy: "sliding" }),
    Stream.mapEffect(Effect.fn(function*(manifest) {
      // TODO: Produce a proper deterministic hash of the manifest.
      const timestamp = yield* Clock.currentTimeMillis
      const version = `${manifest.version}+${timestamp}`
      return new Model.DatasetManifest({ ...manifest, version })
    })),
  )

  // Register and dump datasets when config changes occur.
  yield* Stream.runForEach(configChanges, (manifest) =>
    Effect.gen(function*() {
      // TODO: The server should resolve and dump the dependencies as needed.
      const dependencies = Object.values(manifest.dependencies)
      yield* Effect.forEach(
        dependencies,
        (dependency) => {
          const { name, version } = Model.parseReference(dependency)
          return admin.dumpDatasetVersion(name, version)
        },
        {
          concurrency: "unbounded",
          discard: true,
        },
      )

      yield* admin.registerDataset(manifest.name, manifest.version, manifest)
      yield* admin.dumpDatasetVersion(manifest.name, manifest.version)
    }).pipe(
      Effect.tapError((cause) =>
        Effect.logError(`Failed to dump manifest ${manifest.name}@${manifest.version}`, cause)
      ),
      Effect.ignore,
    ))
})

/**
 * Creates a dev server service layer.
 */
export const layer = () => make.pipe(Layer.scoped(DevServer))
