import { Data, Effect } from "effect"
import * as Api from "./Api.js"
import * as Model from "./Model.js"

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

// TODO: Remove this once the registry endpoint returns the `block_num` field for datasets.
const blockNumField = new Model.ArrowField({
  name: "block_num",
  type: "UInt64",
  nullable: false,
})

export class ManifestBuilder extends Effect.Service<ManifestBuilder>()("Nozzle/ManifestBuilder", {
  effect: Effect.gen(function*() {
    const client = yield* Api.Registry
    const build = (manifest: Model.DatasetDefinition) =>
      Effect.gen(function*() {
        const tables = yield* Effect.forEach(Object.entries(manifest.tables ?? {}), ([name, table]) =>
          Effect.gen(function*() {
            const schema = yield* client.schema(table.sql).pipe(Effect.catchTags({
              RegistryError: (cause) =>
                new ManifestBuilderError({ cause, message: "Failed to get schema", table: name }),
            }))

            if (schema.networks.length != 1) {
              yield* Effect.fail(
                new ManifestBuilderError({
                  cause: undefined,
                  message: `Expected 1 network for SQL query, got ${schema.networks}`,
                  table: name,
                }),
              )
            }

            const input = new Model.TableInput({ sql: table.sql })
            const output = new Model.Table({
              input,
              network: schema.networks[0],
              schema: new Model.TableSchema({
                arrow: new Model.ArrowSchema({
                  fields: [blockNumField, ...schema.schema.arrow.fields],
                }),
              }),
            })

            return [name, output] as const
          }), { concurrency: 5 })

        const functions = Object.entries(manifest.functions ?? {}).map(([name, func]) => {
          const { inputTypes, outputType, source } = func
          const functionManifest = new Model.FunctionManifest({
            name,
            source,
            inputTypes,
            outputType,
          })

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
