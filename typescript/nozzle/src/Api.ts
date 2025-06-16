import { FetchHttpClient, HttpApi, HttpApiClient, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "@effect/platform"
import { Config, Data, Effect, Layer, Schema } from "effect"
import * as Model from "./Model.js"

export class RegistryError extends Data.TaggedError("RegistryError")<{
  readonly cause: unknown
  readonly message: string
}> {}

export class RegistryErrorResponse extends Schema.Class<RegistryErrorResponse>("RegistryErrorResponse")({
  error_code: Schema.Literal("SQL_PARSE_ERROR", "DATASET_STORE_ERROR", "PLANNING_ERROR"),
  error_message: Schema.String,
}) {
  readonly _tag = "RegistryErrorResponse" as const
}

export class RegstistryApiGroup extends HttpApiGroup.make("registry", { topLevel: true }).add(
  HttpApiEndpoint.post("schema")`/output_schema`
    .setPayload(Schema.Struct({ sql_query: Schema.String, is_sql_dataset: Schema.optional(Schema.Boolean) }))
    .addSuccess(Schema.Struct({ schema: Model.TableSchema, networks: Schema.Array(Schema.String) }))
    .addError(RegistryErrorResponse, { status: 400 }) // SQL_PARSE_ERROR
    .addError(RegistryErrorResponse, { status: 500 }), // DATASET_STORE_ERROR & PLANNING_ERROR
) {}

export class RegistryApi extends HttpApi.make("registry").add(RegstistryApiGroup) {}

const makeRegistry = (url: string) =>
  Effect.gen(function*() {
    const client = yield* HttpApiClient.make(RegistryApi, { baseUrl: url })
    const schema = (sql: string, isSqlDataset?: boolean) =>
      client.schema({ payload: { sql_query: sql, is_sql_dataset: isSqlDataset } }).pipe(
        Effect.catchTags({
          RegistryErrorResponse: (cause) => new RegistryError({ cause, message: cause.error_message }),
          HttpApiDecodeError: (cause) => new RegistryError({ cause, message: "Malformed response" }),
          RequestError: (cause) => new RegistryError({ cause, message: "Request error" }),
          ResponseError: (cause) => new RegistryError({ cause, message: "Response error" }),
          ParseError: (cause) => new RegistryError({ cause, message: "Parse error" }),
        }),
      )

    return { schema }
  })

export class Registry extends Effect.Service<Registry>()("Nozzle/Api/Registry", {
  dependencies: [FetchHttpClient.layer],
  effect: Config.string("NOZZLE_REGISTRY_URL").pipe(Effect.flatMap(makeRegistry), Effect.orDie),
}) {
  static withUrl(url: string) {
    return makeRegistry(url).pipe(Effect.map(this.make), Layer.effect(this), Layer.provide(FetchHttpClient.layer))
  }
}

export class AdminError extends Data.TaggedError("AdminError")<{
  readonly cause: unknown
  readonly message: string
}> {}

export class AdminErrorResponse extends Schema.Class<AdminErrorResponse>("AdminErrorResponse")({
  error_code: Schema.Literal("MANIFEST_PARSE_ERROR", "SCHEDULER_ERROR", "DATASET_DEF_STORE_ERROR", "INVALID_MANIFEST"),
  error_message: Schema.String,
}) {
  readonly _tag = "AdminErrorResponse" as const
}

export class AdminApiGroup extends HttpApiGroup.make("admin", { topLevel: true }).add(
  HttpApiEndpoint.post("deploy")`/deploy`
    .setPayload(Schema.Struct({ dataset_name: Schema.String, manifest: Schema.parseJson(Model.DatasetManifest) }))
    .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addError(AdminErrorResponse, { status: 400 }) // MANIFEST_PARSE_ERROR
    .addError(AdminErrorResponse, { status: 500 }), // SCHEDULER_ERROR & DATASET_DEF_STORE_ERROR
) {}

export class AdminApi extends HttpApi.make("admin").add(AdminApiGroup) {}

const makeAdmin = (url: string) =>
  Effect.gen(function*() {
    const client = yield* HttpApiClient.make(AdminApi, { baseUrl: url })
    const deploy = (manifest: Model.DatasetManifest) =>
      client.deploy({ payload: { dataset_name: manifest.name, manifest } }).pipe(
        Effect.catchTags({
          AdminErrorResponse: (cause) => new AdminError({ cause, message: cause.error_message }),
          HttpApiDecodeError: (cause) => new AdminError({ cause, message: "Malformed response" }),
          RequestError: (cause) => new AdminError({ cause, message: "Request error" }),
          ResponseError: (cause) => new AdminError({ cause, message: "Response error" }),
          ParseError: (cause) => new AdminError({ cause, message: "Parse error" }),
        }),
      )

    return { deploy }
  })

export class Admin extends Effect.Service<Admin>()("Nozzle/Api/Admin", {
  dependencies: [FetchHttpClient.layer],
  effect: Config.string("NOZZLE_ADMIN_URL").pipe(Effect.flatMap(makeAdmin), Effect.orDie),
}) {
  static withUrl(url: string) {
    return makeAdmin(url).pipe(Effect.map(this.make), Layer.effect(this), Layer.provide(FetchHttpClient.layer))
  }
}
