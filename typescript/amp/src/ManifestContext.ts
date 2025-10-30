import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as ParseResult from "effect/ParseResult"
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
  dependencies: ReadonlyArray<Model.DatasetReference>
}

export const ManifestContext = Context.GenericTag<DatasetContext>("Amp/ManifestContext")

/**
 * Parse all dependencies from a manifest's dependencies record.
 */
export const parseDependencies = (
  dependencies: Record<string, string>,
): Effect.Effect<ReadonlyArray<Model.DatasetReference>, ParseResult.ParseError> => {
  return Effect.forEach(
    Object.entries(dependencies),
    ([_key, refStr]) => Model.parseDatasetReference(refStr),
    { concurrency: "unbounded" },
  )
}

const fromConfigFile = (file: string) =>
  Effect.gen(function*() {
    const loader = yield* ConfigLoader.ConfigLoader
    const buildResult = yield* loader.build(file).pipe(
      Effect.mapError((cause) => new ManifestContextError({ cause, file, message: "Failed to build config file" })),
    )

    return {
      metadata: buildResult.metadata,
      manifest: buildResult.manifest,
      dependencies: buildResult.dependencies,
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
