import { FileSystem, Path } from "@effect/platform"
import { Data, Effect, Schema } from "effect"
import * as Model from "./Model.js"

export class ManifestLoaderError extends Data.TaggedError("ManifestLoaderError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export class ManifestLoader extends Effect.Service<ManifestLoader>()("Nozzle/ManifestLoader", {
  effect: Effect.gen(function*() {
    const path = yield* Path.Path
    const fs = yield* FileSystem.FileSystem

    const load = Effect.fnUntraced(function*(file: string) {
      return yield* fs.readFileString(path.resolve(file)).pipe(
        Effect.flatMap((content) =>
          Effect.try({
            try: () => JSON.parse(content),
            catch: (cause) => cause,
          })
        ),
        Effect.flatMap(Schema.decodeUnknown(Model.DatasetManifest)),
        Effect.mapError((cause) => new ManifestLoaderError({ cause, message: `Failed to load manifest file ${file}` })),
      )
    })

    return { load }
  }),
}) {}
