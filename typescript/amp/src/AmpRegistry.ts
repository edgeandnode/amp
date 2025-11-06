import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Auth from "./Auth.ts"
import * as Model from "./Model.ts"

const AMP_REGISTRY_API_URL_BASE = new URL("http://localhost:4000")

export class AmpRegistryService extends Effect.Service<AmpRegistryService>()("Amp/Services/AmpRegistryService", {
  dependencies: [FetchHttpClient.layer, Auth.layer],
  effect: Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient
    const auth = yield* Auth.AuthService

    return {} as const
  }),
}) {}

class AmpRegistryInsertDatasetVersionDto
  extends Schema.Class<AmpRegistryInsertDatasetVersionDto>("Amp/Registry/Models/AmpRegistryInsertDatasetVersionDto")({
    status: Schema.Literal("draft", "published"),
    changelog: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
    version_tag: Model.DatasetRevision,
    manifest: Model.DatasetManifest,
    kind: Schema.Literal("manifest", "evm-rpc", "eth-beacon", "firehose"),
    ancestors: Schema.Array(Model.DatasetReference),
  })
{}
class AmpRegistryInsertDatasetDto
  extends Schema.Class<AmpRegistryInsertDatasetDto>("Amp/Registry/Models/AmpRegistryInsertDatasetDto")({
    namespace: Model.DatasetNamespace.pipe(Schema.optionalWith({ nullable: true })),
    name: Model.DatasetName,
    description: Model.DatasetDescription.pipe(Schema.optionalWith({ nullable: true })),
    keywords: Schema.Array(Model.DatasetKeyword).pipe(Schema.optionalWith({ nullable: true })),
    indexing_chains: Schema.Array(Schema.String),
    source: Schema.Array(Schema.String).pipe(Schema.optionalWith({ nullable: true })),
    readme: Model.DatasetReadme.pipe(Schema.optionalWith({ nullable: true })),
    visibility: Schema.Literal("public", "private"),
    repository_url: Model.DatasetRepository.pipe(Schema.optionalWith({ nullable: true })),
    license: Model.DatasetLicense.pipe(Schema.optionalWith({ nullable: true })),
    version: AmpRegistryInsertDatasetVersionDto,
  })
{}
