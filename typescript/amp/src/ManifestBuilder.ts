import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Admin from "./api/Admin.ts"
import * as Model from "./Model.ts"

// Schema for ManifestBuildResult with proper encoding/decoding
export const ManifestBuildResult = Schema.Struct({
  metadata: Model.DatasetMetadata,
  manifest: Model.DatasetDerived,
})
export type ManifestBuildResult = typeof ManifestBuildResult.Type

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
          namespace: config.namespace ?? Model.DatasetNamespace.make("_"),
          name: config.name,
          readme: config.readme,
          repository: config.repository,
          description: config.description,
          keywords: config.keywords,
          license: config.license,
          visibility: config.private ? "private" : "public",
          sources: config.sources,
        })

        // Build manifest tables - send all tables in one request
        const tables = yield* Effect.gen(function*() {
          const configTables = config.tables ?? {}
          const configFunctions = config.functions ?? {}

          // Build function definitions map from config
          const functionsMap = Object.fromEntries(
            Object.entries(configFunctions).map(([name, func]) => [
              name,
              new Model.FunctionDefinition({
                source: func.source,
                inputTypes: func.inputTypes,
                outputType: func.outputType,
              }),
            ]),
          )

          // If no tables and no functions, skip schema request entirely
          if (Object.keys(configTables).length === 0 && Object.keys(functionsMap).length === 0) {
            return []
          }

          // If no tables but we have functions, still skip schema request
          // (when functions-only validation happens server-side, returns empty schema)
          if (Object.keys(configTables).length === 0) {
            return []
          }

          // Prepare all table SQL queries
          const tableSqlMap = Object.fromEntries(
            Object.entries(configTables).map(([name, table]) => [name, table.sql]),
          )

          // Call schema endpoint with all tables and functions at once
          const request = new Admin.GetOutputSchemaPayload({
            tables: tableSqlMap,
            dependencies: config.dependencies,
            functions: Object.keys(functionsMap).length > 0 ? functionsMap : undefined,
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

            const network = Model.Network.make(tableSchema.networks[0])
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
          startBlock: config.startBlock,
          dependencies: config.dependencies,
          tables: Object.fromEntries(tables),
          functions: Object.fromEntries(functions),
        })

        return { metadata, manifest }
      })

    return { build }
  }),
}) {}
