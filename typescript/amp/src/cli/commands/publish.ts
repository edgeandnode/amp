import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

const AMP_REGISTRY_API_URL_BASE = new URL("https://registry.amp.staging.edgeandnode.com")

class AmpRegistryInsertDatasetVersionDto
  extends Schema.Class<AmpRegistryInsertDatasetVersionDto>("AmpCli/Models/AmpRegistryInsertDatasetVersionDto")({
    status: Schema.Literal("draft", "published"),
    changelog: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
    version_tag: Model.DatasetRevision,
    manifest: Model.DatasetDerived,
    ancestors: Schema.Array(Model.DatasetReference),
  })
{}
class AmpRegistryInsertDatasetDto
  extends Schema.Class<AmpRegistryInsertDatasetDto>("AmpCli/Models/AmpRegistryInsertDatasetDto")({
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

export const publish = Command.make("publish", {
  args: {
    tag: Args.text({ name: "tag" }).pipe(
      Args.withDescription("Dataset version (semver) or 'dev' tag"),
      Args.withSchema(Schema.Union(Model.DatasetVersion, Model.DatasetDevTag)),
      Args.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Publish a Dataset to the public registry"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const context = yield* ManifestContext.ManifestContext
      const auth = yield* Auth.AuthService

      const maybeAccessToken = yield* auth.get()
      if (Option.isNone(maybeAccessToken)) {
        return yield* Console.error("Must be authenticated to publish your dataset. Run `amp auth login`")
      }
      const accessToken = maybeAccessToken.value.accessToken

      // If tag is not provided or is "dev", register without version tag (bumps dev tag)
      // Otherwise, register with the specified semantic version
      const version = Option.match(args.tag, {
        onNone: () => Option.none(),
        onSome: (tag) => (Schema.is(Model.DatasetDevTag)(tag) ? Option.none() : Option.some(tag)),
      })

      const publishDatasetToRegistry = Effect.fn("PublishDatasetToRegistry")(function*(
        ctx: ManifestContext.DatasetContext,
        token: string,
      ) {
        const body = yield* HttpBody.json(AmpRegistryInsertDatasetDto.make({
          namespace: ctx.metadata.namespace,
          name: ctx.metadata.name,
          description: ctx.metadata.description,
          keywords: ctx.metadata.keywords,
          indexing_chains: [],
          source: [],
          readme: ctx.metadata.readme,
          visibility: "public",
          repository_url: ctx.metadata.repository,
          license: ctx.metadata.license,
          version: {
            version_tag: "1.0.0",
            status: Option.isSome(version) && version.value === "dev" ? "draft" : "published",
            manifest: ctx.manifest,
            ancestors: [],
          },
        }))
        const req = HttpClientRequest.post(new URL("api/v1/owners/@me/datasets", AMP_REGISTRY_API_URL_BASE)).pipe(
          HttpClientRequest.acceptJson,
          HttpClientRequest.bearerToken(accessToken),
          HttpClientRequest.setHeaders({
            "Content-Type": "application/json",
            "Accept": "application/json",
          }),
          HttpClientRequest.setBody(body),
        )
      })
    })
  ),
  Command.provide(Auth.layer),
  Command.provide(FetchHttpClient.layer),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile)
      .pipe(
        Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
      )
  ),
)
