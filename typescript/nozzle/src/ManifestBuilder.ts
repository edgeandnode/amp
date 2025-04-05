import { Effect } from "effect";
import { Api } from "./Api.js";
import * as Model from "./Model.js";

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Nozzle/ManifestBuilder", {
  dependencies: [Api.Default],
  effect: Effect.gen(function* () {
    const client = yield* Api;
    const build = (manifest: Model.DatasetDefinition) => Effect.gen(function* () {
      const tables = yield* Effect.forEach(Object.entries(manifest.tables), ([name, table]) => Effect.gen(function* () {
        const schema = yield* client.registry.schema({
          payload: { sql_query: table.sql },
        });

        const input = new Model.TableInput({ sql: table.sql });
        const output = new Model.Table({
          input,
          schema: schema.schema,
        });

        return [name, output] as const;
      }), { concurrency: 5 });

      return new Model.DatasetManifest({
        kind: "manifest",
        name: manifest.name,
        version: manifest.version,
        tables: Object.fromEntries(tables),
        dependencies: manifest.dependencies,
      });
    });

    return { build };
  }),
}) {}
