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

export class ManifestBuilderError extends Data.TaggedError(
  "ManifestBuilderError",
)<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()(
  "Amp/ManifestBuilder",
  {
    effect: Effect.gen(function*() {
      const client = yield* Admin.Admin

      const build = Effect.fn("ManifestBuilder.build")(function*(
        config: Model.DatasetConfig,
      ) {
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

        // Extract configuration values
        const configTables = config.tables ?? {}
        const configFunctions = config.functions ?? {}

        // Build function definitions map from config
        const functionsMap: Record<string, Model.FunctionDefinition> = {}
        for (const [name, func] of Object.entries(configFunctions)) {
          functionsMap[name] = new Model.FunctionDefinition({
            source: func.source,
            inputTypes: func.inputTypes,
            outputType: func.outputType,
          })
        }

        // Cache length computations
        const configTablesLength = Object.keys(configTables).length
        const functionsMapLength = Object.keys(functionsMap).length

        // Setup the table map
        const tablesMap: Record<string, Model.Table> = {}

        // Only perform schema request if tables are present - when functions-
        // only validation happens server-side, an empty schema is returned
        if (configTablesLength !== 0) {
          // Prepare all table SQL queries
          const tableSqlMap: Record<string, string> = {}
          for (const [name, table] of Object.entries(configTables)) {
            tableSqlMap[name] = table.sql
          }

          // Call schema endpoint with all tables and functions at once
          const request = new Admin.GetOutputSchemaPayload({
            tables: tableSqlMap,
            dependencies: config.dependencies,
            functions: functionsMapLength > 0 ? functionsMap : undefined,
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
          for (const [name, table] of Object.entries(configTables)) {
            const tableSchema = response.schemas[name]

            if (!tableSchema) {
              return yield* new ManifestBuilderError({
                cause: undefined,
                message: `No schema returned for table ${name}`,
                table: name,
              })
            }

            if (tableSchema.networks.length != 1) {
              return yield* new ManifestBuilderError({
                cause: undefined,
                message: `Expected 1 network for SQL query, got ${tableSchema.networks}`,
                table: name,
              })
            }

            const network = Model.Network.make(tableSchema.networks[0])
            const input = new Model.TableInput({ sql: table.sql })
            const output = new Model.Table({
              input,
              schema: tableSchema.schema,
              network,
            })

            tablesMap[name] = output
          }
        }

        // Build manifest functions
        const functionManifestMap: Record<string, Model.FunctionManifest> = {}
        for (const [name, func] of Object.entries(configFunctions)) {
          const functionManifest = new Model.FunctionManifest({
            name,
            source: func.source,
            inputTypes: func.inputTypes,
            outputType: func.outputType,
          })
          functionManifestMap[name] = functionManifest
        }

        const manifest = new Model.DatasetDerived({
          kind: "manifest",
          dependencies: config.dependencies,
          tables: tablesMap,
          functions: functionManifestMap,
        })

        return { metadata, manifest }
      })

      return { build }
    }),
  },
) {}
