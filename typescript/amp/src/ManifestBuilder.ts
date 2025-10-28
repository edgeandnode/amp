import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Admin from "./api/Admin.ts"
import * as Model from "./Model.ts"

export interface ManifestBuildResult {
  metadata: Model.DatasetMetadata
  manifest: Model.DatasetDerived
}

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Amp/ManifestBuilder", {
  effect: Effect.gen(function*() {
    const client = yield* Admin.Admin
    const build = (definition: Model.DatasetConfig) =>
      Effect.gen(function*() {
        const tables = yield* Effect.forEach(
          Object.entries(definition.tables ?? {}),
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

        const functions = Object.entries(definition.functions ?? {}).map(([name, func]) => {
          const { inputTypes, outputType, source } = func
          const functionManifest = new Model.FunctionManifest({ name, source, inputTypes, outputType })

          return [name, functionManifest] as const
        })

        const metadata = new Model.DatasetMetadata({
          namespace: definition.namespace ?? Model.DEFAULT_NAMESPACE,
          name: definition.name,
          version: definition.version,
        })

        const manifest = new Model.DatasetDerived({
          kind: "manifest",
          dependencies: definition.dependencies,
          tables: Object.fromEntries(tables),
          functions: Object.fromEntries(functions),
        })

        return { metadata, manifest }
      })

    return { build }
  }),
}) {}
