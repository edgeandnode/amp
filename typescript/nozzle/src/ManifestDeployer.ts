import { Data, Effect } from "effect"
import * as Api from "./Api.ts"
import type * as Model from "./Model.ts"

export class ManifestDeployerError extends Data.TaggedError("ManifestDeployerError")<{
  readonly cause: unknown
  readonly message: string
  readonly manifest: string
}> {}

export class ManifestDeployer extends Effect.Service<ManifestDeployer>()("Nozzle/ManifestDeployer", {
  effect: Effect.gen(function*() {
    const client = yield* Api.Admin
    const deploy = (manifest: Model.DatasetManifest) =>
      Effect.gen(function*() {
        const result = yield* client.deploy(manifest).pipe(Effect.catchTags({
          AdminError: (cause) =>
            new ManifestDeployerError({ cause, manifest: manifest.name, message: "Failed to deploy manifest" }),
        }))

        return result
      })

    return { deploy }
  }),
}) {}
