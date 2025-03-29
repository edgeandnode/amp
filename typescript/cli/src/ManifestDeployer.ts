import { Effect } from "effect";
import { Api } from "./Api.js";
import { ManifestBuilder } from "./ManifestBuilder.js";
import type * as Model from "./Model.js";

export class ManifestDeployer extends Effect.Service<ManifestDeployer>()("Nozzle/ManifestDeployer", {
  dependencies: [Api.Default, ManifestBuilder.Default],
  effect: Effect.gen(function* () {
    const client = yield* Api;
    const builder = yield* ManifestBuilder;

    const deploy = (definition: Model.DatasetDefinition) => Effect.gen(function* () {
      const manifest = yield* builder.build(definition);
      yield* client.admin.deploy({
        payload: {
          dataset_name: manifest.name,
          manifest,
        },
      });
    });

    return { deploy };
  }),
}) {}
