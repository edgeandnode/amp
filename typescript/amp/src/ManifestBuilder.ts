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

        // Build manifest tables - send all tables in one request
        const tables = yield* Effect.gen(function*() {
          const configTables = config.tables ?? {}
          const configFunctions = config.functions ?? {}

          // Extract function names from config
          const functionNames = Object.keys(configFunctions)

          // If no tables and no functions, skip schema request entirely
          if (Object.keys(configTables).length === 0 && functionNames.length === 0) {
            return []
          }

          // If no tables but we have functions, still skip schema request
          // (functions-only validation happens server-side, returns empty schema)
          if (Object.keys(configTables).length === 0) {
            return []
          }

          // Prepare all table SQL queries
          const tableSqlMap = Object.fromEntries(
            Object.entries(configTables).map(([name, table]) => [name, table.sql]),
          )

          // Call schema endpoint with all tables and functions at once
          const request = new Model.SchemaRequest({
            tables: tableSqlMap,
            dependencies: config.dependencies,
            functions: functionNames.length > 0 ? functionNames : undefined,
          })

          const response = yield* client.getOutputSchema(request).pipe(
            Effect.catchAll((cause) =>
              Effect.fail(
                new ManifestBuilderError({
                  cause,
                  message: "Failed to get schemas",
                  table: "(all tables)",
                }),
              )
            ),
          )

          // Process each table's schema
          return Object.entries(configTables).map(([name, table]) => {
            const tableSchema = response.schemas[name]
            if (!tableSchema) {
              throw new ManifestBuilderError({
                cause: undefined,
                message: `No schema returned for table ${name}`,
                table: name,
              })
            }

            if (tableSchema.networks.length != 1) {
              throw new ManifestBuilderError({
                cause: undefined,
                message: `Expected 1 network for SQL query, got ${tableSchema.networks}`,
                table: name,
              })
            }

            const network = tableSchema.networks[0]
            const input = new Model.TableInput({ sql: table.sql })
            const output = new Model.Table({ input, schema: tableSchema.schema, network })

            return [name, output] as const
          })
        })

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
