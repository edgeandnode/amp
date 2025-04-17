import { Data, Effect } from "effect";
import * as Api from "./Api.js";
import * as Model from "./Model.js";

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause?: unknown;
  readonly message?: string;
}> {}

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Nozzle/ManifestBuilder", {
  dependencies: [Api.Registry.Default],
  effect: Effect.gen(function* () {
    const client = yield* Api.Registry;
    const build = (manifest: Model.DatasetDefinition) => Effect.gen(function* () {
      const tables = yield* Effect.forEach(Object.entries(manifest.tables), ([name, table]) => Effect.gen(function* () {
        const schema = yield* client.schema({
          payload: { sql_query: table.sql },
        }).pipe(Effect.mapError((cause) => new ManifestBuilderError({
          cause,
          message: `Failed to build table ${name} in manifest ${manifest.name}`,
        })));

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
