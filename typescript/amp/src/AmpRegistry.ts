import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
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

const AMP_REGISTRY_API_URL_BASE = new URL("https://api.registry.amp.staging.edgeandnode.com")

export class AmpRegistryService extends Effect.Service<AmpRegistryService>()("Amp/Services/AmpRegistryService", {
  dependencies: [FetchHttpClient.layer, Auth.layer],
  effect: Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient

    // Helper to build URL
    const buildUrl = (path: string) => new URL(path, AMP_REGISTRY_API_URL_BASE).toString()

    // Helper type for parsed registry errors (never succeeds, always fails with RegistryApiError)
    type ParsedRegistryError = Effect.Effect<never, RegistryApiError, never>

    // Helper to parse registry error responses into RegistryApiError
    const parseRegistryError = (response: HttpClientResponse.HttpClientResponse): ParsedRegistryError =>
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

    // Helper for standard network/response error handling
    const handleHttpClientErrors = <A, R>(effect: Effect.Effect<A, HttpClientError.HttpClientError, R>) =>
      effect.pipe(
        Effect.catchTag("RequestError", (error) =>
          Effect.fail(
            new RegistryApiError({
              status: 0,
              errorCode: "NETWORK_ERROR",
              message: `Failed to connect to registry: ${error.reason}`,
            }),
          )),
        Effect.catchTag("ResponseError", (error) =>
          Effect.fail(
            new RegistryApiError({
              status: 0,
              errorCode: "RESPONSE_ERROR",
              message: `Invalid response from registry: ${error.reason}`,
            }),
          )),
      )

    // Helper for GET requests that return Option (404 -> None)
    const getRequest = <A, I>(
      url: string,
      responseSchema: Schema.Schema<A, I, never>,
      auth?: Auth.AuthStorageSchema,
    ): Effect.Effect<Option.Option<A>, RegistryApiError, never> => {
      const request = HttpClientRequest.get(url, { acceptJson: true })
      const authenticatedRequest = auth
        ? request.pipe(HttpClientRequest.bearerToken(auth.accessToken))
        : request

      return authenticatedRequest.pipe(
        client.execute,
        handleHttpClientErrors,
        Effect.flatMap(
          HttpClientResponse.matchStatus({
            404: () => Effect.succeed(Option.none<A>()),
            ...(auth ? { 401: (response) => parseRegistryError(response) } : {}),
            "2xx": (response) =>
              HttpClientResponse.schemaBodyJson(responseSchema)(response).pipe(
                Effect.map(Option.some),
                Effect.mapError(() =>
                  new RegistryApiError({
                    status: response.status,
                    errorCode: "SCHEMA_VALIDATION_ERROR",
                    message: "Response schema validation failed",
                  })
                ),
              ),
            orElse: (response) => parseRegistryError(response),
          }),
        ),
      ) as Effect.Effect<Option.Option<A>, RegistryApiError, never>
    }

    // Helper for authenticated POST/PUT requests with body
    const makeAuthenticatedRequest = <A, AE, I, IE>(
      method: "POST" | "PUT",
      url: string,
      auth: Auth.AuthStorageSchema,
      bodySchema: Schema.Schema<I, IE, never>,
      body: I,
      responseSchema: Schema.Schema<A, AE, never>,
    ): Effect.Effect<A, RegistryApiError, never> =>
      Effect.gen(function*() {
        const encodedBody = yield* HttpBody.jsonSchema(bodySchema)(body).pipe(
          Effect.mapError((error) =>
            new RegistryApiError({
              status: 0,
              errorCode: "REQUEST_BODY_ERROR",
              message: `Failed to encode request body: ${error.reason}`,
            })
          ),
        )

        const request = method === "POST"
          ? HttpClientRequest.post(url, {
            acceptJson: true,
            headers: { "Content-Type": "application/json" },
          })
          : HttpClientRequest.put(url, {
            acceptJson: true,
            headers: { "Content-Type": "application/json" },
          })

        return yield* request.pipe(
          HttpClientRequest.bearerToken(auth.accessToken),
          HttpClientRequest.setBody(encodedBody),
          client.execute,
          handleHttpClientErrors,
          Effect.flatMap(
            HttpClientResponse.matchStatus({
              "2xx": (response) =>
                HttpClientResponse.schemaBodyJson(responseSchema)(response).pipe(
                  Effect.mapError(() =>
                    new RegistryApiError({
                      status: response.status,
                      errorCode: "SCHEMA_VALIDATION_ERROR",
                      message: "Response schema validation failed",
                    })
                  ),
                ),
              orElse: (response) => parseRegistryError(response),
            }),
          ),
        )
      })

    /**
     * Get a dataset by namespace and name
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @returns Option.some(dataset) if found, Option.none() if not found
     */
    const getDataset = (
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
    ): Effect.Effect<Option.Option<AmpRegistryDatasetDto>, RegistryApiError, never> =>
      getRequest(buildUrl(`api/v1/datasets/${namespace}/${name}`), AmpRegistryDatasetDto)

    /**
     * Get a dataset owned by the authenticated user (including private datasets)
     * @param auth - Authenticated user's auth storage
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @returns Option.some(dataset) if found, Option.none() if not found
     */
    const getOwnedDataset = (
      auth: Auth.AuthStorageSchema,
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
    ): Effect.Effect<Option.Option<AmpRegistryDatasetDto>, RegistryApiError, never> =>
      getRequest(buildUrl(`api/v1/owners/@me/datasets/${namespace}/${name}`), AmpRegistryDatasetDto, auth)

    /**
     * Get dataset by trying public endpoint first, then owned endpoint as fallback
     * @param auth - Auth for owned dataset lookup
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @returns Option of dataset from either endpoint
     */
    const getDatasetWithFallback = (
      auth: Auth.AuthStorageSchema,
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
    ): Effect.Effect<Option.Option<AmpRegistryDatasetDto>, RegistryApiError, never> =>
      getDataset(namespace, name).pipe(
        Effect.flatMap((publicResult) =>
          Option.match(publicResult, {
            onSome: (dataset) => Effect.succeed(Option.some(dataset)),
            onNone: () => getOwnedDataset(auth, namespace, name),
          })
        ),
      )

    /**
     * Publish a new dataset
     * @param auth - Authenticated user's auth storage
     * @param dto - Dataset data to publish
     * @returns Created dataset
     */
    const publishDataset = (
      auth: Auth.AuthStorageSchema,
      dto: AmpRegistryInsertDatasetDto,
    ): Effect.Effect<AmpRegistryDatasetDto, RegistryApiError, never> =>
      makeAuthenticatedRequest(
        "POST",
        buildUrl("api/v1/owners/@me/datasets/publish"),
        auth,
        AmpRegistryInsertDatasetDto,
        dto,
        AmpRegistryDatasetDto,
      )

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
    ): Effect.Effect<AmpRegistryDatasetVersionDto, RegistryApiError, never> =>
      makeAuthenticatedRequest(
        "POST",
        buildUrl(`api/v1/owners/@me/datasets/${namespace}/${name}/versions/publish`),
        auth,
        AmpRegistryInsertDatasetVersionDto,
        dto,
        AmpRegistryDatasetVersionDto,
      )

    /**
     * Update mutable metadata fields on an existing dataset
     * @param auth - Authenticated user's auth storage
     * @param namespace - Dataset namespace
     * @param name - Dataset name
     * @param dto - Metadata update data
     * @returns Updated dataset
     */
    const updateDatasetMetadata = (
      auth: Auth.AuthStorageSchema,
      namespace: Model.DatasetNamespace,
      name: Model.DatasetName,
      dto: AmpRegistryUpdateDatasetMetadataDto,
    ): Effect.Effect<AmpRegistryDatasetDto, RegistryApiError, never> =>
      makeAuthenticatedRequest(
        "PUT",
        buildUrl(`api/v1/owners/@me/datasets/${namespace}/${name}`),
        auth,
        AmpRegistryUpdateDatasetMetadataDto,
        dto,
        AmpRegistryDatasetDto,
      )

    /**
     * Check if metadata has changed between existing dataset and new metadata
     * @param dataset - Existing dataset from registry
     * @param metadata - New metadata from context
     * @param indexingChains - New indexing chains derived from manifest
     * @param source - New source references
     * @returns Effect that succeeds with true if any metadata field has changed
     */
    const hasMetadataChanged = (
      dataset: AmpRegistryDatasetDto,
      metadata: ManifestContext.DatasetContext["metadata"],
      indexingChains: ReadonlyArray<string>,
    ): Effect.Effect<boolean, never, never> =>
      Effect.gen(function*() {
        // Helper to treat null/undefined/empty as equivalent
        const isEmpty = (value: unknown): boolean =>
          value === null || value === undefined || value === "" || (Array.isArray(value) && value.length === 0)

        // Helper to compare optional values
        const optionalChanged = (datasetVal: unknown, metadataVal: unknown): boolean => {
          if (isEmpty(datasetVal) && isEmpty(metadataVal)) return false
          if (isEmpty(datasetVal) || isEmpty(metadataVal)) return true
          return datasetVal !== metadataVal
        }

        // Helper to compare arrays
        const arrayChanged = (
          datasetArr: ReadonlyArray<unknown> | null | undefined,
          metadataArr: ReadonlyArray<unknown> | undefined,
        ): boolean => {
          if (isEmpty(datasetArr) && isEmpty(metadataArr)) return false
          if (isEmpty(datasetArr) || isEmpty(metadataArr)) return true
          const arr1 = datasetArr ?? []
          const arr2 = metadataArr ?? []
          if (arr1.length !== arr2.length) return true
          return arr1.some((val, idx) => val !== arr2[idx])
        }

        // Compare each mutable field
        if (optionalChanged(dataset.description, metadata.description)) return true
        if (arrayChanged(dataset.keywords, metadata.keywords)) return true
        if (optionalChanged(dataset.readme, metadata.readme)) return true
        if (
          optionalChanged(
            dataset.repository_url?.toString(),
            metadata.repository?.toString(),
          )
        ) {
          return true
        }
        if (optionalChanged(dataset.license, metadata.license)) return true
        if (arrayChanged(dataset.source, metadata.sources)) return true
        if (arrayChanged(dataset.indexing_chains, indexingChains)) return true

        return false
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
      }>,
    ) {
      const { auth, changelog, context, versionTag } = args
      const { dependencies, manifest, metadata } = context
      const { description, keywords, license, name, namespace, readme, repository, sources, visibility } = metadata

      // derived from the tables in the dataset manifest (unique chains only)
      const indexingChains = [...new Set(Object.values(manifest.tables).map((table) => table.network))]

      // Check if dataset exists (try public first, then owned/private)
      const maybeDataset = yield* getDatasetWithFallback(auth, namespace, name)

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
                status: "published",
                version_tag: versionTag,
                manifest,
                kind: manifest.kind,
                ancestors: Array.map(dependencies, (dep) =>
                  `${dep.namespace}/${dep.name}@${dep.revision}` as Model.DatasetReferenceStr),
                changelog,
              }),
            )

            // Update metadata if any fields have changed
            const metadataChanged = yield* hasMetadataChanged(dataset, metadata, indexingChains)
            if (metadataChanged) {
              yield* updateDatasetMetadata(
                auth,
                namespace,
                name,
                AmpRegistryUpdateDatasetMetadataDto.make({
                  indexing_chains: indexingChains,
                  description,
                  keywords,
                  readme,
                  repository_url: repository,
                  license,
                  source: sources,
                }),
              )
            }

            // Return dataset reference
            return Model.DatasetReference.make({ namespace, name, revision: versionTag })
          }),
        // Dataset doesn't exist - publish new dataset with initial version
        onNone: () =>
          publishDataset(
            auth,
            AmpRegistryInsertDatasetDto.make({
              namespace,
              name,
              description,
              keywords,
              indexing_chains: indexingChains,
              source: sources,
              readme,
              visibility: visibility ?? "public",
              repository_url: repository,
              license,
              version: AmpRegistryInsertDatasetVersionDto.make({
                status: "published",
                version_tag: versionTag,
                manifest,
                kind: manifest.kind,
                ancestors: Array.map(dependencies, (dep) =>
                  `${dep.namespace}/${dep.name}@${dep.revision}` as Model.DatasetReferenceStr),
                changelog,
              }),
            }),
          ).pipe(Effect.map(() =>
            Model.DatasetReference.make({ namespace, name, revision: versionTag })
          )),
      })
    })

    return {
      getDataset,
      getOwnedDataset,
      publishDataset,
      publishVersion,
      updateDatasetMetadata,
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

export class AmpRegistryUpdateDatasetMetadataDto
  extends Schema.Class<AmpRegistryUpdateDatasetMetadataDto>("Amp/Registry/Models/AmpRegistryUpdateDatasetMetadataDto")({
    indexing_chains: Schema.Array(Schema.String),
    description: Model.DatasetDescription.pipe(Schema.optionalWith({ nullable: true })),
    keywords: Schema.Array(Model.DatasetKeyword).pipe(Schema.optionalWith({ nullable: true })),
    readme: Model.DatasetReadme.pipe(Schema.optionalWith({ nullable: true })),
    repository_url: Model.DatasetRepository.pipe(Schema.optionalWith({ nullable: true })),
    license: Model.DatasetLicense.pipe(Schema.optionalWith({ nullable: true })),
    source: Schema.Array(Schema.String).pipe(Schema.optionalWith({ nullable: true })),
  })
{}
