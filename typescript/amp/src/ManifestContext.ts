import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as ConfigLoader from "./ConfigLoader.ts"
import * as Model from "./Model.ts"

export class ManifestContextError extends Data.TaggedError("ManifestContextError")<{
  readonly message: string
  readonly cause?: unknown
  readonly file?: string
}> {}

export interface DatasetContext {
  metadata: Model.DatasetMetadata
  manifest: Model.DatasetManifest
}

export const ManifestContext = Context.GenericTag<DatasetContext>("Amp/ManifestContext")

const fromManifest = (file: string, metadata: Model.DatasetMetadata) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const resolved = path.resolve(file)

    const manifest = yield* fs.readFileString(resolved).pipe(
      Effect.flatMap((content) => Effect.try({ try: () => JSON.parse(content), catch: (cause) => cause })),
      Effect.flatMap(Schema.decodeUnknown(Model.DatasetManifest)),
      Effect.mapError((cause) =>
        new ManifestContextError({ cause, file: resolved, message: "Failed to load manifest file" })
      ),
    )

    return { metadata, manifest }
  })

const fromConfigFile = (file: string) =>
  Effect.gen(function*() {
    const loader = yield* ConfigLoader.ConfigLoader
    const buildResult = yield* loader.build(file).pipe(
      Effect.mapError((cause) => new ManifestContextError({ cause, file, message: "Failed to build config file" })),
    )
    return {
      metadata: buildResult.metadata,
      manifest: buildResult.manifest,
    }
  })

const fromFile = (options: {
  manifest: Option.Option<string>
  metadata: Option.Option<Model.DatasetMetadata>
  config: Option.Option<string>
}) =>
  Effect.gen(function*() {
    // If manifest provided, metadata must be provided
    if (Option.isSome(options.manifest)) {
      if (Option.isNone(options.metadata)) {
        return yield* Effect.fail(
          new ManifestContextError({
            message:
              "Metadata required when using manifest file. Usage: amp register namespace/name@version --manifest file.json",
          }),
        )
      }
      return yield* fromManifest(options.manifest.value, options.metadata.value)
    }

    // If config provided, use it (metadata ignored)
    if (Option.isSome(options.config)) {
      return yield* fromConfigFile(options.config.value)
    }

    // Try to find config file
    const configLoader = yield* ConfigLoader.ConfigLoader
    const foundConfigPath = yield* configLoader.find()
    if (Option.isSome(foundConfigPath)) {
      return yield* fromConfigFile(foundConfigPath.value)
    }

    // No config or manifest provided
    return yield* Effect.fail(
      new ManifestContextError({
        message:
          "No config or manifest file provided. Either provide a config file or use: amp register namespace/name@version --manifest file.json",
      }),
    )
  })

export const layer = Effect.gen(function*() {
  const configLoader = yield* ConfigLoader.ConfigLoader
  const foundConfigPath = yield* configLoader.find()
  if (Option.isSome(foundConfigPath)) {
    const context = yield* fromConfigFile(foundConfigPath.value)
    return Context.make(ManifestContext, context)
  }

  // No config file found - fail with clear error message
  return yield* Effect.fail(
    new ManifestContextError({
      message:
        "No config file found. Please provide a config file (amp.config.ts) or use: amp register namespace/name@version --manifest file.json",
    }),
  )
}).pipe(
  Layer.effectContext,
  Layer.provide(ConfigLoader.ConfigLoader.Default),
)

export const layerFromConfigFile = (file: Option.Option<string>) =>
  Option.match(file, {
    onSome: (file) => fromConfigFile(file),
    onNone: () =>
      Effect.gen(function*() {
        const configLoader = yield* ConfigLoader.ConfigLoader
        const foundConfigPath = yield* configLoader.find()
        if (Option.isNone(foundConfigPath)) {
          return yield* new ManifestContextError({ message: "Failed to find config file" })
        }

        return yield* fromConfigFile(foundConfigPath.value)
      }),
  }).pipe(
    Effect.map((context) => Context.make(ManifestContext, context)),
    Layer.effectContext,
    Layer.provide(ConfigLoader.ConfigLoader.Default),
  )

export const layerFromFile = (options: {
  metadata: Option.Option<Model.DatasetMetadata>
  manifest: Option.Option<string>
  config: Option.Option<string>
}) =>
  fromFile(options).pipe(
    Effect.map((context) => Context.make(ManifestContext, context)),
    Layer.effectContext,
    Layer.provide(ConfigLoader.ConfigLoader.Default),
  )
