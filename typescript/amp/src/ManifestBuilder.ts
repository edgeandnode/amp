import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Admin from "./api/Admin.ts"
import * as Model from "./Model.ts"

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Amp/ManifestBuilder", {
  effect: Effect.gen(function*() {
    const client = yield* Admin.Admin
    const build = (manifest: Model.DatasetDefinition) =>
      Effect.gen(function*() {
        const tables = yield* Effect.forEach(
          Object.entries(manifest.tables ?? {}),
          ([name, table]) =>
            Effect.gen(function*() {
              const schema = yield* client.getOutputSchema(table.sql, { isSqlDataset: true }).pipe(
                Effect.catchAll((cause) =>
                  new ManifestBuilderError({ cause, message: "Failed to get schema", table: name })
                ),
              )

              if (schema.networks.length != 1) {
                return yield* Effect.fail(
                  new ManifestBuilderError({
                    cause: undefined,
                    message: `Expected 1 network for SQL query, got ${schema.networks}`,
                    table: name,
                  }),
                )
              }

              const network = schema.networks[0]
              const input = new Model.TableInput({ sql: table.sql })
              const output = new Model.Table({ input, schema: schema.schema, network })

              return [name, output] as const
            }),
          { concurrency: 5 },
        )

        const functions = Object.entries(manifest.functions ?? {}).map(([name, func]) => {
          const { inputTypes, outputType, source } = func
          const functionManifest = new Model.FunctionManifest({ name, source, inputTypes, outputType })

          return [name, functionManifest] as const
        })

        return new Model.DatasetManifest({
          kind: "manifest",
          name: manifest.name,
          network: manifest.network,
          version: manifest.version,
          tables: Object.fromEntries(tables),
          functions: Object.fromEntries(functions),
          dependencies: manifest.dependencies,
        })
      })

    return { build }
  }),
}) {}
