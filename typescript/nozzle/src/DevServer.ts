import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Hash from "effect/Hash"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Anvil from "./Anvil.ts"
import * as Admin from "./api/Admin.ts"
import * as Registry from "./api/Registry.ts"
import * as ConfigLoader from "./ConfigLoader.ts"
import * as EvmRpc from "./evm/EvmRpc.ts"
import * as Model from "./Model.js"
import * as Utils from "./Utils.ts"

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
export class DevServer extends Context.Tag("Nozzle/DevServer")<DevServer, void>() {}

/**
 * Creates a dev server service instance.
 */
export const make = Effect.gen(function*() {
  const admin = yield* Admin.Admin
  const registry = yield* Registry.Registry
  const evmRpc = yield* EvmRpc.EvmRpc
  const configLoader = yield* ConfigLoader.ConfigLoader

  // Find the nozzle.config.ts file in current directory
  const configFile = yield* configLoader.find().pipe(
    Effect.flatMap(Option.match({
      onNone: () => new DevServerError({ message: "Could not find nozzle.config.ts file in current directory" }),
      onSome: (configFile) => Effect.succeed(configFile),
    })),
  )

  // Deploy the anvil dataset
  yield* admin.deployDataset(Anvil.dataset.name, Anvil.dataset.version, Anvil.dataset)

  // Observe block changes in a sliding buffer
  const blockChanges = evmRpc.streamBlocks.pipe(
    Stream.map((block) => block.number),
    Stream.buffer({ capacity: 1, strategy: "sliding" }),
  )

  // Observe config changes in a sliding buffer
  const configChanges = configLoader.watch(configFile, {
    onError: (cause) => Effect.logError("Invalid dataset configuration", cause),
  }).pipe(
    Stream.buffer({ capacity: 1, strategy: "sliding" }),
    Stream.map((manifest) => {
      // TODO: Produce a proper deterministic hash of the manifest.
      const hash = Encoding.encodeBase64Url(`${Hash.hash(manifest)}`)
      const version = `${manifest.version}-${hash}`
      return new Model.DatasetManifest({ ...manifest, version })
    }),
    Stream.tap((manifest) =>
      registry.register(manifest).pipe(
        Effect.tapError(() => Effect.logError(`Failed to register manifest ${manifest.name}@${manifest.version}`)),
        Effect.ignore,
      )
    ),
    Stream.tap((manifest) =>
      admin.deployDataset(manifest.name, manifest.version).pipe(
        Effect.tapError(() => Effect.logError(`Failed to deploy manifest ${manifest.name}@${manifest.version}`)),
        Effect.ignore,
      )
    ),
  )

  // Dump datasets to the latest block when either block or config changes occur
  yield* Stream.zipLatest(blockChanges, configChanges).pipe(
    Stream.runForEach(([block, manifest]) =>
      Effect.gen(function*() {
        yield* Effect.logInfo(`Dumping datasets ${manifest.name}@${manifest.version} to block ${block}`)

        const dependencies = Object.values(manifest.dependencies)
        yield* Effect.forEach(dependencies, (dependency) =>
          admin.dumpDataset(dependency.name, {
            version: dependency.version,
            endBlock: Number(block),
          }), {
          concurrency: "unbounded",
          discard: true,
        })

        yield* admin.dumpDataset(manifest.name, {
          version: manifest.version,
          endBlock: Number(block),
        })
      }).pipe(
        Effect.tapErrorCause(Utils.logCauseWith("Failed to dump datasets")),
        Effect.ignore,
      )
    ),
  )
})

/**
 * Creates a dev server service layer.
 */
export const layer = () => make.pipe(Layer.scoped(DevServer))
