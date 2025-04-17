import {
  FetchHttpClient,
  HttpApi,
  HttpApiClient,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "@effect/platform";
import { Config, Effect, Schema } from "effect";
import * as Model from "./Model.js";

export class RegstistryApiGroup extends HttpApiGroup.make("registry", { topLevel: true }).add(
  HttpApiEndpoint.post("schema")`/output_schema`
    .setPayload(Schema.Struct({ sql_query: Schema.String }))
    .addSuccess(Schema.Struct({ schema: Model.TableSchema }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class RegistryApi extends HttpApi.make("registry").add(RegstistryApiGroup) {}

export class Registry extends Effect.Service<Registry>()("Nozzle/Api/Registry", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const url = yield* Config.string("NOZZLE_REGISTRY_URL").pipe(Effect.orDie);
    const registry = yield* HttpApiClient.make(RegistryApi, {
      baseUrl: url,
    });

    return registry;
  }),
}) {}

export class AdminApiGroup extends HttpApiGroup.make("admin", { topLevel: true }).add(
  HttpApiEndpoint.post("deploy")`/deploy`
    .setPayload(Schema.Struct({ dataset_name: Schema.String, manifest: Schema.parseJson(Model.DatasetManifest) }))
    .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class AdminApi extends HttpApi.make("admin").add(AdminApiGroup) {}

export class Admin extends Effect.Service<Admin>()("Nozzle/Api/Admin", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const url = yield* Config.string("NOZZLE_ADMIN_URL").pipe(Effect.orDie);
    const admin = yield* HttpApiClient.make(AdminApi, {
      baseUrl: url,
    });

    return admin;
  }),
}) {}

export class JsonLinesApiGroup extends HttpApiGroup.make("jsonl", { topLevel: true }).add(
  HttpApiEndpoint.post("query")`/`
    .setPayload(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class JsonLinesApi extends HttpApi.make("jsonl").add(JsonLinesApiGroup) {}

export class JsonLines extends Effect.Service<JsonLines>()("Nozzle/Api/JsonLines", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const url = yield* Config.string("NOZZLE_JSONL_URL").pipe(Effect.orDie);
    const jsonl = yield* HttpApiClient.make(JsonLinesApi, {
      baseUrl: url,
    });

    return jsonl;
  }),
}) {}
