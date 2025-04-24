import { FetchHttpClient, HttpApi, HttpApiClient, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "@effect/platform"
import { Config, Effect, Layer, Schema } from "effect"
import * as Model from "./Model.js"

export class RegstistryApiGroup extends HttpApiGroup.make("registry", { topLevel: true }).add(
  HttpApiEndpoint.post("schema")`/output_schema`
    .setPayload(Schema.Struct({ sql_query: Schema.String }))
    .addSuccess(Schema.Struct({ schema: Model.TableSchema }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class RegistryApi extends HttpApi.make("registry").add(RegstistryApiGroup) {}

const makeRegistry = (url: string) => HttpApiClient.make(RegistryApi, { baseUrl: url })
export class Registry extends Effect.Service<Registry>()("Nozzle/Api/Registry", {
  dependencies: [FetchHttpClient.layer],
  effect: Config.string("NOZZLE_REGISTRY_URL").pipe(Effect.flatMap(makeRegistry), Effect.orDie),
}) {
  static withUrl(url: string) {
    return makeRegistry(url).pipe(Effect.map(this.make), Layer.effect(this), Layer.provide(FetchHttpClient.layer))
  }
}

export class AdminApiGroup extends HttpApiGroup.make("admin", { topLevel: true }).add(
  HttpApiEndpoint.post("deploy")`/deploy`
    .setPayload(Schema.Struct({ dataset_name: Schema.String, manifest: Schema.parseJson(Model.DatasetManifest) }))
    .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class AdminApi extends HttpApi.make("admin").add(AdminApiGroup) {}

const makeAdmin = (url: string) => HttpApiClient.make(AdminApi, { baseUrl: url })
export class Admin extends Effect.Service<Admin>()("Nozzle/Api/Admin", {
  dependencies: [FetchHttpClient.layer],
  effect: Config.string("NOZZLE_ADMIN_URL").pipe(Effect.flatMap(makeAdmin), Effect.orDie),
}) {
  static withUrl(url: string) {
    return makeAdmin(url).pipe(Effect.map(this.make), Layer.effect(this), Layer.provide(FetchHttpClient.layer))
  }
}
