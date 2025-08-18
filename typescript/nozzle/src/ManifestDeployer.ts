import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Admin from "./api/Admin.ts"
import type * as Model from "./Model.ts"

export class ManifestDeployerError extends Data.TaggedError("ManifestDeployerError")<{
  readonly cause: unknown
  readonly message: string
  readonly manifest: string
}> {}

export class ManifestDeployer extends Effect.Service<ManifestDeployer>()("Nozzle/ManifestDeployer", {
  effect: Effect.gen(function*() {
    const client = yield* Admin.Admin
    const deploy = (manifest: Model.DatasetManifest) =>
      Effect.gen(function*() {
        const result = yield* client.deployDataset(manifest).pipe(Effect.catchAll((cause) =>
          new ManifestDeployerError({ cause, manifest: manifest.name, message: "Failed to deploy manifest" })
        ))

        return result
      })

    return { deploy }
  }),
}) {}
