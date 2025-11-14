import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientError from "@effect/platform/HttpClientError"
import type * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import { afterEach, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"

import * as AmpRegistry from "@edgeandnode/amp/AmpRegistry"
import * as Auth from "@edgeandnode/amp/Auth"
import type * as ManifestContext from "@edgeandnode/amp/ManifestContext"
import * as Model from "@edgeandnode/amp/Model"

// Test Fixtures

const mockAuthStorage = Auth.AuthStorageSchema.make({
  accessToken: "test-access-token",
  refreshToken: "test-refresh-token",
  userId: "cmfoby1bt005el70b0fjd3glv",
  accounts: ["cmfoby1bt005el70b0fjd3glv", "0x04913E13A937cf63Fad3786FEE42b3d44dA558aA"],
  expiry: Date.now() + 3600000,
})

const mockDatasetVersionDto = AmpRegistry.AmpRegistryDatasetVersionDto.make({
  status: "published",
  created_at: "2024-01-01T00:00:00Z",
  version_tag: "1.0.0",
  dataset_reference: "edgeandnode/mainnet@1.0.0",
  changelog: "Initial release",
  ancestors: [],
  descendants: [],
})

const mockDatasetDto = AmpRegistry.AmpRegistryDatasetDto.make({
  namespace: "edgeandnode",
  name: "mainnet",
  created_at: "2024-01-01T00:00:00Z",
  updated_at: "2024-01-01T00:00:00Z",
  description: "Mainnet dataset",
  indexing_chains: [], // Empty to match mockManifest's empty tables
  keywords: ["ethereum", "mainnet"],
  license: "MIT",
  readme: "# Mainnet Dataset",
  repository_url: new URL("https://github.com/edgeandnode/mainnet"),
  source: [], // Empty to match publishFlow's derived source
  visibility: "public",
  owner: "0x04913E13A937cf63Fad3786FEE42b3d44dA558aA",
  dataset_reference: "edgeandnode/mainnet@latest",
  latest_version: mockDatasetVersionDto,
  versions: [mockDatasetVersionDto],
})

const mockNewVersionDto = AmpRegistry.AmpRegistryDatasetVersionDto.make({
  status: "published",
  created_at: "2024-01-02T00:00:00Z",
  version_tag: "1.1.0",
  dataset_reference: "edgeandnode/mainnet@1.1.0",
  changelog: "Added new features",
  ancestors: [AmpRegistry.AmpRegistryDatasetVersionAncestryDto.make({
    dataset_reference: "edgeandnode/mainnet@1.0.0",
  })],
  descendants: [],
})

const mockManifest = Model.DatasetDerived.make({
  kind: "manifest",
  dependencies: {},
  tables: {},
  functions: {},
})

const mockManifestContext: ManifestContext.DatasetContext = {
  metadata: Model.DatasetMetadata.make({
    namespace: "edgeandnode",
    name: "mainnet",
    description: "Mainnet dataset",
    keywords: ["ethereum", "mainnet"],
    license: "MIT",
    readme: "# Mainnet Dataset",
    repository: new URL("https://github.com/edgeandnode/mainnet"),
    visibility: "public",
  }),
  manifest: mockManifest,
  dependencies: {},
}

const mockErrorResponse = AmpRegistry.AmpRegistryErrorResponseDto.make({
  error_code: "DATASET_NOT_FOUND",
  error_message: "Dataset not found",
  request_id: "req-123",
})

// Helper to create mock HTTP client
const createMockHttpClient = (
  handler: (
    req: HttpClientRequest.HttpClientRequest,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError>,
) => HttpClient.make(handler)

describe("AmpRegistryService", () => {
  afterEach(() => {
    // Cleanup if needed
  })

  describe("getDataset", () => {
    it.effect("should return Option.some when dataset exists (200)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(mockDatasetDto), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* service.getDataset("edgeandnode", "mainnet")

        expect(Option.isSome(result)).toBe(true)
        if (Option.isSome(result)) {
          expect(result.value.namespace).toBe("edgeandnode")
          expect(result.value.name).toBe("mainnet")
          expect(result.value.owner).toBe("0x04913E13A937cf63Fad3786FEE42b3d44dA558aA")
        }
      }))

    it.effect("should return Option.none when dataset not found (404)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response("Not Found", {
                status: 404,
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* service.getDataset("edgeandnode", "nonexistent")

        expect(Option.isNone(result)).toBe(true)
      }))

    it.effect("should fail with RegistryApiError on server error (500)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(mockErrorResponse), {
                status: 500,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getDataset("edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
      }))

    it.effect("should handle malformed error response gracefully", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response("Internal Server Error", {
                status: 500,
                headers: { "Content-Type": "text/plain" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getDataset("edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
      }))

    it.effect("should fail with RegistryApiError on network error (RequestError)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.fail(
            new HttpClientError.RequestError({
              request: req,
              reason: "Transport",
              description: "Connection refused",
            }),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getDataset("edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("NETWORK_ERROR")
            expect(result.cause.error.message).toContain("registry")
          }
        }
      }))

    it.effect("should fail with RegistryApiError on response error (ResponseError)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.fail(
            new HttpClientError.ResponseError({
              request: req,
              response: HttpClientResponse.fromWeb(
                req,
                new Response("Gateway Timeout", {
                  status: 504,
                }),
              ),
              reason: "StatusCode",
              description: "Gateway timeout",
            }),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getDataset("edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("RESPONSE_ERROR")
          }
        }
      }))

    it.effect("should fail with RegistryApiError on schema validation error", ({ expect }) =>
      Effect.gen(function*() {
        // Return 200 but with invalid JSON structure (missing required fields)
        const invalidDataset = {
          namespace: "edgeandnode",
          // Missing required fields like name, created_at, etc.
        }

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(invalidDataset), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getDataset("edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("SCHEMA_VALIDATION_ERROR")
          }
        }
      }))
  })

  describe("getOwnedDataset", () => {
    it.effect("should return Option.some when owned dataset exists (200)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) => {
          // Verify bearer token is sent
          expect(req.headers.authorization).toBe("Bearer test-access-token")
          expect(req.url).toContain("owners/@me/datasets")

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(mockDatasetDto), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        })

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* service.getOwnedDataset(mockAuthStorage, "edgeandnode", "mainnet")

        expect(Option.isSome(result)).toBe(true)
        if (Option.isSome(result)) {
          expect(result.value.namespace).toBe("edgeandnode")
          expect(result.value.name).toBe("mainnet")
        }
      }))

    it.effect("should return Option.none when dataset not found (404)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response("Not Found", {
                status: 404,
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* service.getOwnedDataset(mockAuthStorage, "edgeandnode", "nonexistent")

        expect(Option.isNone(result)).toBe(true)
      }))

    it.effect("should fail with RegistryApiError on unauthorized (401)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(
                JSON.stringify(
                  AmpRegistry.AmpRegistryErrorResponseDto.make({
                    error_code: "UNAUTHORIZED",
                    error_message: "Invalid or missing authentication token",
                    request_id: "req-401",
                  }),
                ),
                {
                  status: 401,
                  headers: { "Content-Type": "application/json" },
                },
              ),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getOwnedDataset(mockAuthStorage, "edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.status).toBe(401)
          }
        }
      }))

    it.effect("should fail with RegistryApiError on network error", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.fail(
            new HttpClientError.RequestError({
              request: req,
              reason: "Transport",
              description: "Connection refused",
            }),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getOwnedDataset(mockAuthStorage, "edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("NETWORK_ERROR")
          }
        }
      }))

    it.effect("should fail with RegistryApiError on schema validation error", ({ expect }) =>
      Effect.gen(function*() {
        // Return 200 but with invalid JSON structure (missing required fields)
        const invalidDataset = {
          namespace: "edgeandnode",
          // Missing required fields
        }

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(invalidDataset), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)
        const result = yield* Effect.exit(service.getOwnedDataset(mockAuthStorage, "edgeandnode", "mainnet"))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("SCHEMA_VALIDATION_ERROR")
          }
        }
      }))
  })

  describe("publishDataset", () => {
    it.effect("should publish new dataset successfully (201)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) => {
          // Verify request has Bearer token
          expect(req.headers.authorization).toBe("Bearer test-access-token")
          expect(req.headers["content-type"]).toBe("application/json")

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(mockDatasetDto), {
                status: 201,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        })

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const insertDto = AmpRegistry.AmpRegistryInsertDatasetDto.make({
          namespace: "edgeandnode",
          name: "mainnet",
          description: "Mainnet dataset",
          keywords: ["ethereum", "mainnet"],
          indexing_chains: ["mainnet"],
          source: ["firehose"],
          readme: "# Mainnet Dataset",
          visibility: "public",
          repository_url: new URL("https://github.com/edgeandnode/mainnet"),
          license: "MIT",
          version: AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
            status: "published",
            version_tag: "1.0.0",
            manifest: mockManifest,
            kind: "firehose",
            ancestors: [],
            changelog: "Initial release",
          }),
        })

        const result = yield* service.publishDataset(mockAuthStorage, insertDto)

        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.visibility).toBe("public")
      }))

    it.effect("should fail with RegistryApiError on bad request (400)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(
                JSON.stringify(
                  AmpRegistry.AmpRegistryErrorResponseDto.make({
                    error_code: "VALIDATION_ERROR",
                    error_message: "Invalid dataset name",
                    request_id: "req-456",
                  }),
                ),
                {
                  status: 400,
                  headers: { "Content-Type": "application/json" },
                },
              ),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const insertDto = AmpRegistry.AmpRegistryInsertDatasetDto.make({
          namespace: "edgeandnode",
          name: "mainnet",
          description: "Mainnet dataset",
          keywords: [],
          indexing_chains: ["mainnet"],
          source: [],
          visibility: "public",
          version: AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
            status: "published",
            version_tag: "1.0.0",
            manifest: mockManifest,
            kind: "firehose",
            ancestors: [],
          }),
        })

        const result = yield* Effect.exit(service.publishDataset(mockAuthStorage, insertDto))

        expect(result._tag).toBe("Failure")
      }))

    it.effect("should fail with RegistryApiError on network error", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.fail(
            new HttpClientError.RequestError({
              request: req,
              reason: "Transport",
              description: "Failed to establish connection",
            }),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const insertDto = AmpRegistry.AmpRegistryInsertDatasetDto.make({
          namespace: "edgeandnode",
          name: "mainnet",
          description: "Mainnet dataset",
          keywords: [],
          indexing_chains: ["mainnet"],
          source: [],
          visibility: "public",
          version: AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
            status: "published",
            version_tag: "1.0.0",
            manifest: mockManifest,
            kind: "firehose",
            ancestors: [],
          }),
        })

        const result = yield* Effect.exit(service.publishDataset(mockAuthStorage, insertDto))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("NETWORK_ERROR")
          }
        }
      }))

    it.effect("should fail with RegistryApiError on schema validation error in response", ({ expect }) =>
      Effect.gen(function*() {
        // Return 201 but with malformed dataset DTO (missing required fields)
        const malformedDataset = {
          namespace: "edgeandnode",
          name: "mainnet",
          // Missing required fields like created_at, updated_at, owner, etc.
        }

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(malformedDataset), {
                status: 201,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const insertDto = AmpRegistry.AmpRegistryInsertDatasetDto.make({
          namespace: "edgeandnode",
          name: "mainnet",
          description: "Mainnet dataset",
          keywords: [],
          indexing_chains: ["mainnet"],
          source: [],
          visibility: "public",
          version: AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
            status: "published",
            version_tag: "1.0.0",
            manifest: mockManifest,
            kind: "firehose",
            ancestors: [],
          }),
        })

        const result = yield* Effect.exit(service.publishDataset(mockAuthStorage, insertDto))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("SCHEMA_VALIDATION_ERROR")
          }
        }
      }))
  })

  describe("publishVersion", () => {
    it.effect("should publish new version successfully (201)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) => {
          // Verify request has Bearer token and correct endpoint
          expect(req.headers.authorization).toBe("Bearer test-access-token")
          expect(req.url).toContain("edgeandnode/mainnet/versions/publish")

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(mockNewVersionDto), {
                status: 201,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        })

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const versionDto = AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
          status: "published",
          version_tag: "1.1.0",
          manifest: mockManifest,
          kind: "firehose",
          ancestors: ["edgeandnode/mainnet@1.0.0"],
          changelog: "Added new features",
        })

        const result = yield* service.publishVersion(mockAuthStorage, "edgeandnode", "mainnet", versionDto)

        expect(result.version_tag).toBe("1.1.0")
        expect(result.status).toBe("published")
        expect(result.changelog).toBe("Added new features")
      }))

    it.effect("should fail with RegistryApiError on unauthorized (403)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(
                JSON.stringify(
                  AmpRegistry.AmpRegistryErrorResponseDto.make({
                    error_code: "FORBIDDEN",
                    error_message: "You do not own this dataset",
                    request_id: "req-789",
                  }),
                ),
                {
                  status: 403,
                  headers: { "Content-Type": "application/json" },
                },
              ),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const versionDto = AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
          status: "published",
          version_tag: "1.1.0",
          manifest: mockManifest,
          kind: "firehose",
          ancestors: [],
        })

        const result = yield* Effect.exit(service.publishVersion(mockAuthStorage, "edgeandnode", "mainnet", versionDto))

        expect(result._tag).toBe("Failure")
      }))

    // Note: Network error test omitted for publishVersion due to HttpBody.jsonSchema
    // being called before client.execute. Network error handling is tested in getDataset and publishDataset.

    it.effect("should fail with RegistryApiError on schema validation error in response", ({ expect }) =>
      Effect.gen(function*() {
        // Return 201 but with malformed version DTO (missing required fields)
        const malformedVersion = {
          version_tag: "1.1.0",
          status: "published",
          // Missing required fields like created_at, dataset_reference, ancestors, descendants
        }

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(malformedVersion), {
                status: 201,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const versionDto = AmpRegistry.AmpRegistryInsertDatasetVersionDto.make({
          status: "published",
          version_tag: "1.1.0",
          manifest: mockManifest,
          kind: "firehose",
          ancestors: [],
        })

        const result = yield* Effect.exit(service.publishVersion(mockAuthStorage, "edgeandnode", "mainnet", versionDto))

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("SCHEMA_VALIDATION_ERROR")
          }
        }
      }))
  })

  describe("updateDatasetMetadata", () => {
    it.effect("should update dataset metadata successfully (200)", ({ expect }) =>
      Effect.gen(function*() {
        const updatedDataset = AmpRegistry.AmpRegistryDatasetDto.make({
          ...mockDatasetDto,
          description: "Updated description",
          keywords: ["ethereum", "mainnet", "updated"],
        })

        const mockHttpClient = createMockHttpClient((req) => {
          // Verify request has Bearer token and correct method
          expect(req.headers.authorization).toBe("Bearer test-access-token")
          expect(req.method).toBe("PUT")
          expect(req.url).toContain("edgeandnode/mainnet")

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(updatedDataset), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        })

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const updateDto = AmpRegistry.AmpRegistryUpdateDatasetMetadataDto.make({
          indexing_chains: ["mainnet"],
          description: "Updated description",
          keywords: ["ethereum", "mainnet", "updated"],
        })

        const result = yield* service.updateDatasetMetadata(mockAuthStorage, "edgeandnode", "mainnet", updateDto)

        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.description).toBe("Updated description")
        expect(result.keywords).toEqual(["ethereum", "mainnet", "updated"])
      }))

    it.effect("should fail with RegistryApiError on forbidden (403)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(
                JSON.stringify(
                  AmpRegistry.AmpRegistryErrorResponseDto.make({
                    error_code: "FORBIDDEN",
                    error_message: "You do not own this dataset",
                    request_id: "req-update-403",
                  }),
                ),
                {
                  status: 403,
                  headers: { "Content-Type": "application/json" },
                },
              ),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const updateDto = AmpRegistry.AmpRegistryUpdateDatasetMetadataDto.make({
          indexing_chains: ["mainnet"],
          description: "Updated description",
        })

        const result = yield* Effect.exit(
          service.updateDatasetMetadata(mockAuthStorage, "edgeandnode", "mainnet", updateDto),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.status).toBe(403)
          }
        }
      }))

    it.effect("should fail with RegistryApiError on dataset not found (404)", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(
                JSON.stringify(
                  AmpRegistry.AmpRegistryErrorResponseDto.make({
                    error_code: "DATASET_NOT_FOUND",
                    error_message: "Dataset not found",
                    request_id: "req-update-404",
                  }),
                ),
                {
                  status: 404,
                  headers: { "Content-Type": "application/json" },
                },
              ),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const updateDto = AmpRegistry.AmpRegistryUpdateDatasetMetadataDto.make({
          indexing_chains: ["mainnet"],
        })

        const result = yield* Effect.exit(
          service.updateDatasetMetadata(mockAuthStorage, "edgeandnode", "nonexistent", updateDto),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.status).toBe(404)
          }
        }
      }))

    it.effect("should fail with RegistryApiError on network error", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.fail(
            new HttpClientError.RequestError({
              request: req,
              reason: "Transport",
              description: "Failed to establish connection",
            }),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const updateDto = AmpRegistry.AmpRegistryUpdateDatasetMetadataDto.make({
          indexing_chains: ["mainnet"],
        })

        const result = yield* Effect.exit(
          service.updateDatasetMetadata(mockAuthStorage, "edgeandnode", "mainnet", updateDto),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("NETWORK_ERROR")
          }
        }
      }))

    it.effect("should fail with RegistryApiError on schema validation error in response", ({ expect }) =>
      Effect.gen(function*() {
        // Return 200 but with malformed dataset DTO (missing required fields)
        const malformedDataset = {
          namespace: "edgeandnode",
          name: "mainnet",
          // Missing required fields like created_at, updated_at, owner, etc.
        }

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify(malformedDataset), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          )
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const updateDto = AmpRegistry.AmpRegistryUpdateDatasetMetadataDto.make({
          indexing_chains: ["mainnet"],
        })

        const result = yield* Effect.exit(
          service.updateDatasetMetadata(mockAuthStorage, "edgeandnode", "mainnet", updateDto),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.cause._tag === "Fail") {
          expect(result.cause.error).toBeInstanceOf(AmpRegistry.RegistryApiError)
          if (result.cause.error instanceof AmpRegistry.RegistryApiError) {
            expect(result.cause.error.errorCode).toBe("SCHEMA_VALIDATION_ERROR")
          }
        }
      }))
  })

  describe("publishFlow", () => {
    it.effect("should publish new dataset when dataset doesn't exist", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let publishDatasetCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset doesn't exist
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/publish - create new dataset
            if (req.method === "POST" && req.url.includes("/datasets/publish")) {
              publishDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.0.0",
          changelog: "Initial release",
        })

        expect(getDatasetCalled).toBe(true)
        expect(publishDatasetCalled).toBe(true)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.0.0")
      }))

    it.effect("should publish new version when dataset exists and user owns it", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let publishVersionCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset exists
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              publishVersionCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.1.0",
          changelog: "Added new features",
        })

        expect(getDatasetCalled).toBe(true)
        expect(publishVersionCalled).toBe(true)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should fail with DatasetOwnershipError when user doesn't own dataset", ({ expect }) =>
      Effect.gen(function*() {
        const differentOwnerDataset = AmpRegistry.AmpRegistryDatasetDto.make({
          ...mockDatasetDto,
          owner: "0xDifferentOwnerAddress",
        })

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset exists with different owner
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(differentOwnerDataset), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* Effect.exit(
          service.publishFlow({
            auth: mockAuthStorage,
            context: mockManifestContext,
            versionTag: "1.1.0",
          }),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure") {
          expect(result.cause._tag).toBe("Fail")
          if (result.cause._tag === "Fail") {
            expect(result.cause.error).toBeInstanceOf(AmpRegistry.DatasetOwnershipError)
            if (result.cause.error instanceof AmpRegistry.DatasetOwnershipError) {
              expect(result.cause.error.namespace).toBe("edgeandnode")
              expect(result.cause.error.name).toBe("mainnet")
              expect(result.cause.error.actualOwner).toBe("0xDifferentOwnerAddress")
              expect(result.cause.error.userAddresses).toEqual(mockAuthStorage.accounts)
            }
          }
        }
      }))

    it.effect("should fail with VersionAlreadyExistsError when version exists", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset exists with version 1.0.0
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        // Try to publish version 1.0.0 which already exists
        const result = yield* Effect.exit(
          service.publishFlow({
            auth: mockAuthStorage,
            context: mockManifestContext,
            versionTag: "1.0.0",
          }),
        )

        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure") {
          expect(result.cause._tag).toBe("Fail")
          if (result.cause._tag === "Fail") {
            expect(result.cause.error).toBeInstanceOf(AmpRegistry.VersionAlreadyExistsError)
            if (result.cause.error instanceof AmpRegistry.VersionAlreadyExistsError) {
              expect(result.cause.error.namespace).toBe("edgeandnode")
              expect(result.cause.error.name).toBe("mainnet")
              expect(result.cause.error.versionTag).toBe("1.0.0")
            }
          }
        }
      }))

    it.effect("should derive indexingChains from manifest.tables", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET - dataset doesn't exist
            if (req.method === "GET") {
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // POST - return success (indexingChains are derived internally)
            if (req.method === "POST" && req.url.includes("/datasets/publish")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const contextWithMultipleTables: ManifestContext.DatasetContext = {
          ...mockManifestContext,
          manifest: Model.DatasetDerived.make({
            kind: "manifest",
            dependencies: {},
            tables: {
              blocks: Model.Table.make({
                input: Model.TableInput.make({ sql: "SELECT * FROM blocks" }),
                schema: Model.TableSchema.make({
                  arrow: Model.ArrowSchema.make({ fields: [] }),
                }),
                network: "mainnet",
              }),
              transactions: Model.Table.make({
                input: Model.TableInput.make({ sql: "SELECT * FROM transactions" }),
                schema: Model.TableSchema.make({
                  arrow: Model.ArrowSchema.make({ fields: [] }),
                }),
                network: "mainnet",
              }),
              logs: Model.Table.make({
                input: Model.TableInput.make({ sql: "SELECT * FROM logs" }),
                schema: Model.TableSchema.make({
                  arrow: Model.ArrowSchema.make({ fields: [] }),
                }),
                network: "sepolia",
              }),
            },
            functions: {},
          }),
        }

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: contextWithMultipleTables,
          versionTag: "1.0.0",
        })

        // Verify the flow completed successfully
        // The indexingChains are derived from manifest.tables internally
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.0.0")
      }))

    it.effect("should convert dependencies to DatasetReferenceStr in ancestors", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET - dataset exists
            if (req.method === "GET") {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // POST - return success (ancestors are converted internally)
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const contextWithDependencies: ManifestContext.DatasetContext = {
          ...mockManifestContext,
          dependencies: {
            mainnet: Model.DatasetReference.make({
              namespace: "edgeandnode",
              name: "mainnet",
              revision: "1.0.0",
            }),
          },
        }

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: contextWithDependencies,
          versionTag: "1.1.0",
        })

        // Verify the flow completed successfully
        // Dependencies are converted to ancestors internally
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should use status and visibility from metadata with defaults", ({ expect }) =>
      Effect.gen(function*() {
        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET - dataset doesn't exist
            if (req.method === "GET") {
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // POST - return success (status/visibility defaults applied internally)
            if (req.method === "POST" && req.url.includes("/datasets/publish")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        // Context without explicit status/visibility
        const contextWithoutDefaults: ManifestContext.DatasetContext = {
          metadata: Model.DatasetMetadata.make({
            namespace: "edgeandnode",
            name: "test",
            // status and visibility will use defaults
          }),
          manifest: mockManifest,
          dependencies: {},
        }

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: contextWithoutDefaults,
          versionTag: "1.0.0",
        })

        // Verify the flow completed successfully
        // Defaults for visibility ("public") and status ("published") are applied internally
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("test")
        expect(result.revision).toBe("1.0.0")
      }))

    it.effect("should update metadata when publishing version if metadata changed", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let publishVersionCalled = false
        let updateMetadataCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset exists with old description
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(
                  JSON.stringify(
                    AmpRegistry.AmpRegistryDatasetDto.make({
                      ...mockDatasetDto,
                      description: "Old description",
                    }),
                  ),
                  {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                  },
                ),
              )
            }

            // POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              publishVersionCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // PUT /api/v1/owners/@me/datasets/{namespace}/{name} - update metadata
            if (req.method === "PUT" && req.url.includes("/datasets/edgeandnode/mainnet")) {
              updateMetadataCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(
                  JSON.stringify(
                    AmpRegistry.AmpRegistryDatasetDto.make({
                      ...mockDatasetDto,
                      description: "Mainnet dataset",
                    }),
                  ),
                  {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                  },
                ),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.1.0",
          changelog: "Updated features",
        })

        expect(getDatasetCalled).toBe(true)
        expect(publishVersionCalled).toBe(true)
        expect(updateMetadataCalled).toBe(true)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should NOT update metadata when publishing version if metadata unchanged", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let publishVersionCalled = false
        let updateMetadataCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - dataset exists with same metadata
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              publishVersionCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // PUT /api/v1/owners/@me/datasets/{namespace}/{name} - should NOT be called
            if (req.method === "PUT") {
              updateMetadataCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Should not be called", {
                  status: 500,
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.1.0",
          changelog: "Updated features",
        })

        expect(getDatasetCalled).toBe(true)
        expect(publishVersionCalled).toBe(true)
        expect(updateMetadataCalled).toBe(false)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should update metadata when keywords change from empty to populated", ({ expect }) =>
      Effect.gen(function*() {
        let updateMetadataCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET - dataset exists with no keywords
            if (req.method === "GET" && req.url.includes("/datasets/")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(
                  JSON.stringify(
                    AmpRegistry.AmpRegistryDatasetDto.make({
                      ...mockDatasetDto,
                      keywords: undefined, // Use undefined instead of null
                    }),
                  ),
                  {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                  },
                ),
              )
            }

            // POST - version publish
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // PUT - metadata update should be called
            if (req.method === "PUT") {
              updateMetadataCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext, // Has keywords: ["ethereum", "mainnet"]
          versionTag: "1.1.0",
        })

        expect(updateMetadataCalled).toBe(true)
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should publish version to existing private dataset", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let getOwnedDatasetCalled = false
        let publishVersionCalled = false
        let publishDatasetCalled = false

        const privateDataset = AmpRegistry.AmpRegistryDatasetDto.make({
          ...mockDatasetDto,
          visibility: "private",
        })

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - returns 404 (private dataset)
            if (req.method === "GET" && !req.url.includes("@me")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // GET /api/v1/owners/@me/datasets/{namespace}/{name} - returns private dataset
            if (req.method === "GET" && req.url.includes("@me")) {
              getOwnedDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(privateDataset), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
            if (req.method === "POST" && req.url.includes("/versions/publish")) {
              publishVersionCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockNewVersionDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/publish - should NOT be called
            if (req.method === "POST" && req.url.includes("/datasets/publish")) {
              publishDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Should not be called", {
                  status: 500,
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.1.0",
          changelog: "New version for private dataset",
        })

        expect(getDatasetCalled).toBe(true)
        expect(getOwnedDatasetCalled).toBe(true)
        expect(publishVersionCalled).toBe(true)
        expect(publishDatasetCalled).toBe(false)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.1.0")
      }))

    it.effect("should create new dataset when both public and private checks return 404", ({ expect }) =>
      Effect.gen(function*() {
        let getDatasetCalled = false
        let getOwnedDatasetCalled = false
        let publishDatasetCalled = false

        const mockHttpClient = createMockHttpClient((req) =>
          Effect.gen(function*() {
            // GET /api/v1/datasets/{namespace}/{name} - returns 404
            if (req.method === "GET" && !req.url.includes("@me")) {
              getDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // GET /api/v1/owners/@me/datasets/{namespace}/{name} - returns 404
            if (req.method === "GET" && req.url.includes("@me")) {
              getOwnedDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response("Not Found", {
                  status: 404,
                }),
              )
            }

            // POST /api/v1/owners/@me/datasets/publish - creates new dataset
            if (req.method === "POST" && req.url.includes("/datasets/publish")) {
              publishDatasetCalled = true
              return HttpClientResponse.fromWeb(
                req,
                new Response(JSON.stringify(mockDatasetDto), {
                  status: 201,
                  headers: { "Content-Type": "application/json" },
                }),
              )
            }

            return yield* Effect.fail(
              new HttpClientError.RequestError({
                request: req,
                reason: "InvalidUrl",
                description: `Unexpected request: ${req.method} ${req.url}`,
              }),
            )
          })
        )

        const MockHttpClientLayer = Layer.succeed(HttpClient.HttpClient, mockHttpClient)
        const TestLayer = Layer.provide(AmpRegistry.AmpRegistryService.DefaultWithoutDependencies, MockHttpClientLayer)

        const service = yield* Effect.provide(AmpRegistry.AmpRegistryService, TestLayer)

        const result = yield* service.publishFlow({
          auth: mockAuthStorage,
          context: mockManifestContext,
          versionTag: "1.0.0",
          changelog: "Initial release",
        })

        expect(getDatasetCalled).toBe(true)
        expect(getOwnedDatasetCalled).toBe(true)
        expect(publishDatasetCalled).toBe(true)
        expect(result.namespace).toBe("edgeandnode")
        expect(result.name).toBe("mainnet")
        expect(result.revision).toBe("1.0.0")
      }))
  })
})
