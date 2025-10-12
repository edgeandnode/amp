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

export const ManifestContext = Context.GenericTag<Model.DatasetManifest>("Amp/ManifestContext")

const fromManifest = (file: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const resolved = path.resolve(file)

    return yield* fs.readFileString(resolved).pipe(
      Effect.flatMap((content) => Effect.try({ try: () => JSON.parse(content), catch: (cause) => cause })),
      Effect.flatMap(Schema.decodeUnknown(Model.DatasetManifest)),
      Effect.mapError((cause) =>
        new ManifestContextError({ cause, file: resolved, message: "Failed to load manifest file" })
      ),
    )
  })

const fromConfigFile = (file: string) =>
  Effect.gen(function*() {
    const loader = yield* ConfigLoader.ConfigLoader
    return yield* loader.build(file)
  }).pipe(
    Effect.mapError((cause) => new ManifestContextError({ cause, file, message: "Failed to load config file" })),
  )

const fromFile = (options: {
  manifest: Option.Option<string>
  config: Option.Option<string>
}) =>
  Effect.gen(function*() {
    if (Option.isSome(options.manifest)) {
      return yield* fromManifest(options.manifest.value)
    }

    if (Option.isSome(options.config)) {
      return yield* fromConfigFile(options.config.value)
    }

    const configLoader = yield* ConfigLoader.ConfigLoader
    const foundConfigPath = yield* configLoader.find()
    if (Option.isSome(foundConfigPath)) {
      return yield* fromConfigFile(foundConfigPath.value)
    }

    return yield* fromManifest("amp.json")
  })

export const layer = Effect.gen(function*() {
  const configLoader = yield* ConfigLoader.ConfigLoader
  const foundConfigPath = yield* configLoader.find()
  if (Option.isSome(foundConfigPath)) {
    return yield* fromConfigFile(foundConfigPath.value)
  }

  return yield* fromManifest("amp.json")
}).pipe(
  Layer.effect(ManifestContext),
  Layer.provide(ConfigLoader.ConfigLoader.Default),
)

export const layerFromManifestFile = (file: Option.Option<string>) =>
  Option.match(file, {
    onSome: (file) => fromManifest(file),
    onNone: () => fromManifest("amp.json"),
  }).pipe(
    Layer.effect(ManifestContext),
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
    Layer.effect(ManifestContext),
    Layer.provide(ConfigLoader.ConfigLoader.Default),
  )

export const layerFromFile = (options: {
  manifest: Option.Option<string>
  config: Option.Option<string>
}) =>
  fromFile(options).pipe(
    Layer.effect(ManifestContext),
    Layer.provide(ConfigLoader.ConfigLoader.Default),
  )
