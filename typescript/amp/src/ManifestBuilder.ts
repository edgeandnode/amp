import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Admin from "./api/Admin.ts"
import * as ManifestContext from "./ManifestContext.ts"
import * as Model from "./Model.ts"

export interface ManifestBuildResult {
  metadata: Model.DatasetMetadata
  manifest: Model.DatasetDerived
  dependencies: ReadonlyArray<Model.DatasetReference>
}

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Amp/ManifestBuilder", {
  effect: Effect.gen(function*() {
    const client = yield* Admin.Admin
    const build = (config: Model.DatasetConfig) =>
      Effect.gen(function*() {
        // Extract metadata
        const metadata = new Model.DatasetMetadata({
          namespace: config.namespace ?? Model.DEFAULT_NAMESPACE,
          name: config.name,
          readme: config.readme,
          repository: config.repository,
          description: config.description,
          keywords: config.keywords,
          license: config.license,
          visibility: config.private ? "private" : "public",
        })

        // Build manifest tables
        const tables = yield* Effect.forEach(
          Object.entries(config.tables ?? {}),
          ([name, table]) =>
            Effect.gen(function*() {
              const schema = yield* client.getOutputSchema(table.sql).pipe(
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

        // Build manifest functions
        const functions = Object.entries(config.functions ?? {}).map(([name, func]) => {
          const { inputTypes, outputType, source } = func
          const functionManifest = new Model.FunctionManifest({ name, source, inputTypes, outputType })

          return [name, functionManifest] as const
        })

        const manifest = new Model.DatasetDerived({
          kind: "manifest",
          dependencies: config.dependencies,
          tables: Object.fromEntries(tables),
          functions: Object.fromEntries(functions),
        })

        // Parse all dependencies
        const dependencies = yield* ManifestContext.parseDependencies(config.dependencies)

        return { metadata, manifest, dependencies }
      })

    return { build }
  }),
}) {}
