import { Effect } from "effect";
import { Api } from "./Api.js";
import type * as Model from "./Model.js";

export class ManifestDeployer extends Effect.Service<ManifestDeployer>()("Nozzle/ManifestDeployer", {
  dependencies: [Api.Default],
  effect: Effect.gen(function* () {
    const client = yield* Api;
    const deploy = (manifest: Model.DatasetManifest) => Effect.gen(function* () {
      const result = yield* client.admin.deploy({
        payload: {
          dataset_name: manifest.name,
          manifest,
        },
      });

      return result;
    });

    return { deploy };
  }),
}) {}
