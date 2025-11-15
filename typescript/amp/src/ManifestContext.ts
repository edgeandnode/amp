import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ConfigLoader from "./ConfigLoader.ts"
import type * as Model from "./Model.ts"

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
