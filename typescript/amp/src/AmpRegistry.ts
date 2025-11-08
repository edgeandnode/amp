import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import * as Array from "effect/Array"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Auth from "./Auth.ts"
import type * as ManifestContext from "./ManifestContext.ts"
import * as Model from "./Model.ts"

const AMP_REGISTRY_API_URL_BASE = new URL("http://localhost:4000")

export class AmpRegistryService extends Effect.Service<AmpRegistryService>()("Amp/Services/AmpRegistryService", {
  dependencies: [FetchHttpClient.layer, Auth.layer],
  effect: Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient

    // Helper to build URL
    const buildUrl = (path: string) => new URL(path, AMP_REGISTRY_API_URL_BASE).toString()

    // Helper to parse registry error responses into RegistryApiError
    const parseRegistryError = (response: HttpClientResponse.HttpClientResponse) =>
      response.json.pipe(
        Effect.flatMap(Schema.decodeUnknown(AmpRegistryErrorResponseDto)),
        Effect.flatMap((err) =>
          Effect.fail(
            new RegistryApiError({
              status: response.status,
              errorCode: err.error_code,
              message: err.error_message,
              requestId: err.request_id ?? undefined,
            }),
          )
        ),
        Effect.catchAll(() =>
          Effect.fail(
            new RegistryApiError({
              status: response.status,
              errorCode: "UNKNOWN_ERROR",
              message: `HTTP ${response.status}`,
            }),
          )
        ),
      )

    /**
     * Get a dataset by namespace and name
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @returns Option.some(dataset) if found, Option.none() if not found
     */
    const getDataset = (namespace: Model.DatasetNamespace, name: Model.DatasetName) =>
      HttpClientRequest.get(buildUrl(`api/v1/datasets/${namespace}/${name}`), { acceptJson: true }).pipe(
        client.execute,
        Effect.flatMap((response) =>
          Effect.if(response.status === 404, {
            onTrue: () => Effect.succeed(Option.none<AmpRegistryDatasetDto>()),
            onFalse: () =>
              HttpClientResponse.schemaBodyJson(AmpRegistryDatasetDto)(response).pipe(
                Effect.map(Option.some),
                Effect.catchAll(() => parseRegistryError(response)),
              ),
          })
        ),
      )

    /**
     * Publish a new dataset
     * @param auth - Authenticated user's auth storage
     * @param dto - Dataset data to publish
     * @returns Created dataset
     */
    const publishDataset = (auth: Auth.AuthStorageSchema, dto: AmpRegistryInsertDatasetDto) =>
      Effect.gen(function*() {
        const body = yield* HttpBody.jsonSchema(AmpRegistryInsertDatasetDto)(dto)
        return yield* HttpClientRequest.post(buildUrl("api/v1/owners/@me/datasets/publish"), {
          acceptJson: true,
          headers: {
            "Content-Type": "application/json",
          },
        }).pipe(
          HttpClientRequest.bearerToken(auth.accessToken),
          HttpClientRequest.setBody(body),
          client.execute,
          Effect.flatMap((response) =>
            response.status !== 201
              ? parseRegistryError(response)
              : HttpClientResponse.schemaBodyJson(AmpRegistryDatasetDto)(response).pipe(
                Effect.mapError(() =>
                  new RegistryApiError({
                    status: response.status,
                    errorCode: "SCHEMA_VALIDATION_ERROR",
                    message: "Response schema validation failed",
                  })
                ),
              )
          ),
        )
      })

    /**
     * Publish a new version to an existing dataset
     * @param auth - Authenticated user's auth storage
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @param dto - Version data to publish
     * @returns Created version
     */
    const publishVersion = (
      auth: Auth.AuthStorageSchema,
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
      dto: AmpRegistryInsertDatasetVersionDto,
    ) =>
      Effect.gen(function*() {
        const body = yield* HttpBody.jsonSchema(AmpRegistryInsertDatasetVersionDto)(dto)
        return yield* HttpClientRequest.post(
          buildUrl(`api/v1/owners/@me/datasets/${namespace}/${name}/versions/publish`),
          {
            acceptJson: true,
            headers: {
              "Content-Type": "application/json",
            },
          },
        ).pipe(
          HttpClientRequest.bearerToken(auth.accessToken),
          HttpClientRequest.setBody(body),
          client.execute,
          Effect.flatMap((response) =>
            response.status !== 201
              ? parseRegistryError(response)
              : HttpClientResponse.schemaBodyJson(AmpRegistryDatasetVersionDto)(response).pipe(
                Effect.mapError(() =>
                  new RegistryApiError({
                    status: response.status,
                    errorCode: "SCHEMA_VALIDATION_ERROR",
                    message: "Response schema validation failed",
                  })
                ),
              )
          ),
        )
      })

    /**
     * High-level orchestration method to publish a dataset or version
     * @param auth - Authenticated user's auth storage
     * @param context - Dataset context containing metadata and manifest
     * @param versionTag - Version tag/revision to publish
     * @param indexingChains - Array of indexing chains
     * @param source - Array of source references
     * @param changelog - Optional changelog for the version
     * @returns DatasetReference for the published dataset/version
     */
    const publishFlow = Effect.fn("DatasetPublishFlow")(function*(
      args: Readonly<{
        auth: Auth.AuthStorageSchema
        context: ManifestContext.DatasetContext
        versionTag: Model.DatasetRevision
        changelog?: string | undefined
        status: "published" | "draft"
      }>,
    ) {
      const { auth, changelog, context, status, versionTag } = args
      const { dependencies, manifest, metadata } = context
      const { description, keywords, license, name, namespace, readme, repository, visibility } = metadata

      // derived from the tables in the dataset manifest
      const indexingChains = Object.values(manifest.tables).map((table) => table.network)
      /** @todo figure out a way to derive from the manifest or ask the user */
      const source: ReadonlyArray<string> = []

      // Check if dataset exists
      const maybeDataset = yield* getDataset(namespace, name)

      return yield* Option.match(maybeDataset, {
        // Dataset exists - validate ownership and publish new version
        onSome: (dataset) =>
          Effect.gen(function*() {
            // Validate ownership
            const accounts = auth.accounts ?? []
            const isOwner = accounts.some((account) => account.toLowerCase() === dataset.owner.toLowerCase())

            if (!isOwner) {
              return yield* Effect.fail(
                new DatasetOwnershipError({
                  namespace,
                  name,
                  actualOwner: dataset.owner,
                  userAddresses: accounts,
                }),
              )
            }

            // Check if version already exists
            // const versionExists = dataset.versions?.some((v) => v.version_tag === versionTag) ?? false
            const versionExists = Array.findFirst(dataset.versions ?? [], (v) => v.version_tag === versionTag)
            if (Option.isSome(versionExists)) {
              return yield* Effect.fail(
                new VersionAlreadyExistsError({
                  namespace,
                  name,
                  versionTag,
                }),
              )
            }

            // Publish new version
            yield* publishVersion(
              auth,
              namespace,
              name,
              AmpRegistryInsertDatasetVersionDto.make({
                status,
                version_tag: versionTag,
                manifest,
                kind: manifest.kind,
                ancestors: Array.map(dependencies, (dep) =>
                  `${dep.namespace}/${dep.name}@${dep.revision}` as Model.DatasetReferenceStr),
                changelog,
              }),
            )

            // Return dataset reference
            return Model.DatasetReference.make({ namespace, name, revision: versionTag })
          }),
        // Dataset doesn't exist - publish new dataset with initial version
        onNone: () =>
          Effect.gen(function*() {
            yield* publishDataset(
              auth,
              AmpRegistryInsertDatasetDto.make({
                namespace,
                name,
                description,
                keywords,
                indexing_chains: indexingChains,
                source,
                readme,
                visibility: visibility ?? "public",
                repository_url: repository,
                license,
                version: AmpRegistryInsertDatasetVersionDto.make({
                  status,
                  version_tag: versionTag,
                  manifest,
                  kind: manifest.kind,
                  ancestors: Array.map(dependencies, (dep) =>
                    `${dep.namespace}/${dep.name}@${dep.revision}` as Model.DatasetReferenceStr),
                  changelog,
                }),
              }),
            )

            // Return dataset reference
            return Model.DatasetReference.make({ namespace, name, revision: versionTag })
          }),
      })
    })

    return {
      getDataset,
      publishDataset,
      publishVersion,
      publishFlow,
    } as const
  }),
}) {}
export const layer = AmpRegistryService.Default

// Response DTOs from Amp Registry API

export class AmpRegistryDatasetVersionAncestryDto extends Schema.Class<AmpRegistryDatasetVersionAncestryDto>(
  "Amp/Registry/Models/AmpRegistryDatasetVersionAncestryDto",
)({
  dataset_reference: Model.DatasetReferenceStr,
}) {}

export class AmpRegistryDatasetVersionDto
  extends Schema.Class<AmpRegistryDatasetVersionDto>("Amp/Registry/Models/AmpRegistryDatasetVersionDto")({
    status: Schema.Literal("draft", "published", "deprecated", "archived"),
    created_at: Schema.String,
    version_tag: Model.DatasetRevision,
    dataset_reference: Model.DatasetReferenceStr,
    changelog: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
    ancestors: Schema.Array(AmpRegistryDatasetVersionAncestryDto).pipe(Schema.optionalWith({ nullable: true })),
    descendants: Schema.Array(AmpRegistryDatasetVersionAncestryDto).pipe(Schema.optionalWith({ nullable: true })),
  })
{}

export class AmpRegistryDatasetDto
  extends Schema.Class<AmpRegistryDatasetDto>("Amp/Registry/Models/AmpRegistryDatasetDto")({
    namespace: Model.DatasetNamespace,
    name: Model.DatasetName,
    created_at: Schema.String,
    updated_at: Schema.String,
    description: Model.DatasetDescription.pipe(Schema.optionalWith({ nullable: true })),
    indexing_chains: Schema.Array(Schema.String),
    keywords: Schema.Array(Model.DatasetKeyword).pipe(Schema.optionalWith({ nullable: true })),
    license: Model.DatasetLicense.pipe(Schema.optionalWith({ nullable: true })),
    readme: Model.DatasetReadme.pipe(Schema.optionalWith({ nullable: true })),
    repository_url: Model.DatasetRepository.pipe(Schema.optionalWith({ nullable: true })),
    source: Schema.Array(Schema.String).pipe(Schema.optionalWith({ nullable: true })),
    visibility: Schema.Literal("private", "public"),
    owner: Schema.Union(Schema.NonEmptyTrimmedString, Model.Address),
    dataset_reference: Model.DatasetReferenceStr.pipe(Schema.optionalWith({ nullable: true })),
    latest_version: AmpRegistryDatasetVersionDto.pipe(Schema.optionalWith({ nullable: true })),
    versions: Schema.Array(AmpRegistryDatasetVersionDto).pipe(Schema.optionalWith({ nullable: true })),
  })
{}

export class AmpRegistryErrorResponseDto
  extends Schema.Class<AmpRegistryErrorResponseDto>("Amp/Registry/Models/AmpRegistryErrorResponseDto")({
    error_code: Schema.String,
    error_message: Schema.String,
    request_id: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
  })
{}

// Error Types for Amp Registry Service

export class DatasetAlreadyExistsError extends Data.TaggedError("Amp/Registry/Errors/DatasetAlreadyExistsError")<{
  readonly namespace: string
  readonly name: string
}> {}

export class DatasetNotFoundError extends Data.TaggedError("Amp/Registry/Errors/DatasetNotFoundError")<{
  readonly namespace: string
  readonly name: string
}> {}

export class VersionAlreadyExistsError extends Data.TaggedError("Amp/Registry/Errors/VersionAlreadyExistsError")<{
  readonly namespace: string
  readonly name: string
  readonly versionTag: string
}> {}

export class DatasetOwnershipError extends Data.TaggedError("Amp/Registry/Errors/DatasetOwnershipError")<{
  readonly namespace: string
  readonly name: string
  readonly actualOwner: string
  readonly userAddresses: ReadonlyArray<string>
}> {}

export class RegistryApiError extends Data.TaggedError("Amp/Registry/Errors/RegistryApiError")<{
  readonly status: number
  readonly errorCode: string
  readonly message: string
  readonly requestId?: string | null | undefined
}> {}

export class AmpRegistryInsertDatasetVersionDto
  extends Schema.Class<AmpRegistryInsertDatasetVersionDto>("Amp/Registry/Models/AmpRegistryInsertDatasetVersionDto")({
    status: Schema.Literal("draft", "published"),
    changelog: Schema.String.pipe(Schema.optionalWith({ nullable: true })),
    version_tag: Model.DatasetRevision,
    manifest: Model.DatasetManifest,
    kind: Schema.Literal("manifest", "evm-rpc", "eth-beacon", "firehose"),
    ancestors: Schema.Array(Model.DatasetReferenceStr),
  })
{}

export class AmpRegistryInsertDatasetDto
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
