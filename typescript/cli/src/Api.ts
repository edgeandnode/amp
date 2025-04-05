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

class RegstistryApi extends HttpApiGroup.make("registry", {
  topLevel: true,
}).add(
  HttpApiEndpoint.post("schema")`/output_schema`
    .setPayload(Schema.Struct({ sql_query: Schema.String }))
    .addSuccess(Schema.Struct({ schema: Model.TableSchema }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class Registry extends HttpApi.make("registry").add(RegstistryApi) {}

class AdminApi extends HttpApiGroup.make("admin", { topLevel: true }).add(
  HttpApiEndpoint.post("deploy")`/deploy`
    .setPayload(Schema.Struct({ dataset_name: Schema.String, manifest: Schema.parseJson(Model.DatasetManifest) }))
    .addSuccess(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }))
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 404 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 422 })
    .addError(HttpApiSchema.withEncoding(Schema.String, { kind: "Text" }), { status: 500 }),
) {}

export class Admin extends HttpApi.make("admin").add(AdminApi) {}

export class Api extends Effect.Service<Api>()("Nozzle/Api", {
  dependencies: [FetchHttpClient.layer],
  effect: Effect.gen(function* () {
    const config = yield* Config.all({
      registry: Config.string("NOZZLE_REGISTRY_URL").pipe(
        Config.withDefault("http://localhost:1611"),
      ),
      admin: Config.string("NOZZLE_ADMIN_URL").pipe(
        Config.withDefault("http://localhost:1610"),
      ),
    });

    const { registry, admin } = yield* Effect.all({
      registry: HttpApiClient.make(Registry, {
        baseUrl: config.registry,
      }),
      admin: HttpApiClient.make(Admin, {
        baseUrl: config.admin,
      }),
    });

    return { registry, admin };
  }),
}) {}
