import { Effect } from "effect"
import * as Api from "./Api.js"
import type * as Model from "./Model.js"

export class ManifestDeployer extends Effect.Service<ManifestDeployer>()("Nozzle/ManifestDeployer", {
  effect: Effect.gen(function*() {
    const client = yield* Api.Admin
    const deploy = (manifest: Model.DatasetManifest) =>
      Effect.gen(function*() {
        const result = yield* client.deploy({
          payload: {
            dataset_name: manifest.name,
            manifest,
          },
        })

        return result
      })

    return { deploy }
  }),
}) {}
