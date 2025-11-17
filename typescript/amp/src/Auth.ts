import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Clock from "effect/Clock"
import * as Data from "effect/Data"
import * as DateTime from "effect/DateTime"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as jose from "jose"
import { LocalCache } from "./LocalCache.ts"
import * as Model from "./Model.ts"

export const AUTH_PLATFORM_URL = new URL("https://auth.amp.thegraph.com/")

// =============================================================================
// Access Tokens
// =============================================================================

export const AccessTokenDuration = Schema.Union(
  Schema.Number,
  Schema.DateFromSelf,
  Schema.String.pipe(
    Schema.pattern(
      /^-?\d+\.?\d*\s*(sec|secs|second|seconds|s|minute|minutes|min|mins|m|hour|hours|hr|hrs|h|day|days|d|week|weeks|w|year|years|yr|yrs|y)(\s+ago|\s+from\s+now)?$/i,
    ),
  ),
)

export class GenerateAccessTokenRequest extends Schema.Class<GenerateAccessTokenRequest>(
  "Amp/models/auth/GenerateAccessTokenRequest",
)({
  duration: AccessTokenDuration.pipe(Schema.optionalWith({ nullable: true })),
  audience: Schema.Array(Schema.String).pipe(Schema.optionalWith({ nullable: true })),
}) {}

export class GenerateAccessTokenResponse extends Schema.Class<GenerateAccessTokenResponse>(
  "Amp/models/auth/GenerateAccessTokenResponse",
)({
  token: Schema.NonEmptyTrimmedString,
  token_type: Schema.Literal("Bearer"),
  exp: Schema.Int.pipe(Schema.positive()),
  sub: Schema.NonEmptyTrimmedString,
  iss: Schema.String,
}) {}

export class GenerateAccessTokenError extends Data.TaggedError("Amp/errors/auth/GenerateAccessTokenError")<{
  readonly error: string
  readonly error_description: string
  readonly status: number
}> {}

// =============================================================================
// Refresh Tokens
// =============================================================================

const AuthUserId = Schema.NonEmptyTrimmedString.pipe(
  Schema.pattern(/^(c[a-z0-9]{24}|did:privy:c[a-z0-9]{24})$/),
)

export class RefreshTokenRequest extends Schema.Class<RefreshTokenRequest>(
  "Amp/models/auth/RefreshTokenRequest",
)({
  refresh_token: Model.RefreshToken,
  user_id: AuthUserId,
}) {
  static fromCache(cache: AuthStorageSchema) {
    return RefreshTokenRequest.make({
      user_id: cache.userId,
      refresh_token: cache.refreshToken,
    })
  }
}

export class RefreshTokenResponse extends Schema.Class<RefreshTokenResponse>(
  "Amp/models/auth/RefreshTokenResponse",
)({
  token: Schema.NonEmptyTrimmedString.annotations({
    identifier: "RefreshTokenResponse.token",
    description: "The refreshed access token",
  }),
  refresh_token: Schema.NullOr(Schema.String),
  session_update_action: Schema.String,
  expires_in: Schema.Int.pipe(Schema.positive()).annotations({
    description: "Seconds from receipt of when the token expires (def is 1hr)",
  }),
  user: Schema.Struct({
    id: AuthUserId,
    accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Model.Address)).annotations({
      description: "List of accounts (connected wallets, etc) belonging to the user",
      examples: [["cmfd6bf6u006vjx0b7xb2eybx", "0x5c8fA0bDf68C915a88cD68291fC7CF011C126C29"]],
    }),
  }).annotations({
    description: "The user the access token belongs to",
  }),
}) {}

// =============================================================================
// Token Cache
// =============================================================================

export class AuthStorageSchema extends Schema.Class<AuthStorageSchema>("Amp/models/auth/AuthStorageSchema")({
  accessToken: Model.AccessToken,
  refreshToken: Model.RefreshToken,
  userId: AuthUserId,
  accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Model.Address)).pipe(Schema.optional),
  expiry: Schema.Int.pipe(Schema.positive(), Schema.optional),
}) {}

// =============================================================================
// Errors
// =============================================================================

export class AuthTokenExpiredError extends Data.TaggedError("Amp/errors/auth/AuthTokenExpiredError") {}

export class AuthRateLimitError extends Data.TaggedError("Amp/errors/auth/AuthRateLimitError")<{
  readonly retryAfter: number
  readonly message: string
}> {}

export class AuthRefreshError extends Data.TaggedError("Amp/errors/auth/AuthRefreshError")<{
  readonly status: number
  readonly message: string
}> {}

export class AuthUserMismatchError extends Data.TaggedError("Amp/errors/auth/AuthUserMismatchError")<{
  readonly expected: string
  readonly received: string
}> {}

export class VerifySignedAccessTokenError extends Data.TaggedError("Amp/errors/auth/VerifySignedAccessTokenError")<{
  readonly cause: unknown
}> {}

export class AuthService extends Effect.Service<AuthService>()("Amp/AuthService", {
  dependencies: [LocalCache, FetchHttpClient.layer],
  effect: Effect.gen(function*() {
    const store = yield* KeyValueStore.KeyValueStore
    const kvs = store.forSchema(AuthStorageSchema)

    // Setup the `${AUTH_PLATFORM_URL}/v1/auth` base URL for the HTTP client
    const v1AuthUrl = new URL("v1/auth", AUTH_PLATFORM_URL)
    const httpClient = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest(HttpClientRequest.prependUrl(v1AuthUrl.toString())),
    )

    const AUTH_TOKEN_CACHE_KEY = "amp_cli_auth"

    // ------------------------------------------------------------------------
    // Token Operations
    // ------------------------------------------------------------------------

    const generateAccessToken = Effect.fn("AuthService.generateAccessToken")(function*(args: {
      readonly storedAuth: AuthStorageSchema
      readonly exp: Model.GenrateTokenDuration | undefined
      readonly audience: ReadonlyArray<string> | null | undefined
    }) {
      const request = HttpClientRequest.post("/auth/generate", { 
        // Unsafely creating the JSON body is acceptable here as the requisite 
        // parameters will have already been validated by other schemas
        body: HttpBody.unsafeJson({
          duration: args.exp || undefined,
          audience: args.audience || undefined,
        }),
        acceptJson: true, 
      }).pipe(HttpClientRequest.bearerToken(args.storedAuth.accessToken))

      return yield* httpClient.execute(request).pipe(
        Effect.flatMap(HttpClientResponse.matchStatus({
          "2xx": (response) => HttpClientResponse.schemaBodyJson(GenerateAccessTokenResponse)(response),
          orElse: Effect.fnUntraced(function*(response) {
            // Parse error response from API, with fallback for non-JSON responses
            const errorResponse = yield* response.json.pipe(Effect.catchAll(() => Effect.succeed({})))
            const error = typeof errorResponse === "object" && errorResponse !== null && "error" in errorResponse
              ? String(errorResponse.error)
              : "server_error"
            const error_description =
              typeof errorResponse === "object" && errorResponse !== null && "error_description" in errorResponse
                ? String(errorResponse.error_description)
                : "Failed to generate access token"

            return yield* new GenerateAccessTokenError({
              error,
              error_description,
              status: response.status,
            })
          }),
        })),
      )
    })

    // Helper to extract error description from response body
    const extractErrorDescription = (response: HttpClientResponse.HttpClientResponse) =>
      response.json.pipe(
        Effect.option,
        Effect.map(
          Option.flatMap((body) =>
            typeof body === "object" && body !== null &&
              "error_description" in body && typeof body.error_description === "string"
              ? Option.some(body.error_description)
              : Option.none()
          ),
        ),
        Effect.map(Option.getOrElse(() => "Failed to refresh token")),
      )

    const refreshAccessToken = Effect.fn("AuthService.refreshAccessToken")(function*(
      cache: AuthStorageSchema,
    ) {
      const request = HttpClientRequest.post("/auth/refresh", {
        // Unsafely creating the JSON body is acceptable here as the `user_id`
        // and `refresh_token` have already been validated by the `AuthStorageSchema`
        body: HttpBody.unsafeJson(RefreshTokenRequest.fromCache(cache)),
        acceptJson: true,
      }).pipe(HttpClientRequest.bearerToken(cache.accessToken))

      const response = yield* httpClient.execute(request).pipe(
        Effect.timeout(Duration.seconds(15)),
        Effect.flatMap(HttpClientResponse.matchStatus({
          "2xx": (response) => HttpClientResponse.schemaBodyJson(RefreshTokenResponse)(response),
          // Unauthorized
          401: () => Effect.fail(new AuthTokenExpiredError()),
          // Insufficient Permissions
          403: () => Effect.fail(new AuthTokenExpiredError()),
          // Too Many Requests
          429: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            const retryAfter = Option.fromNullable(response.headers["retry-after"]).pipe(
              Option.flatMap((retryAfter) => {
                const parsed = Number.parseInt(retryAfter, 10)
                return Number.isNaN(parsed) ? Option.none() : Option.some(parsed)
              }),
              Option.getOrElse(() => 60),
            )
            return yield* new AuthRateLimitError({ message, retryAfter })
          }),
          orElse: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            return yield* new AuthRefreshError({ message, status: response.status })
          }),
        })),
      )

      // Validate that the received user ID matches the cached user ID
      if (response.user.id !== cache.userId) {
        return yield* new AuthUserMismatchError({
          expected: cache.userId,
          received: response.user.id,
        })
      }

      const now = yield* DateTime.now

      const expiry = DateTime.toEpochMillis(DateTime.add(now, { seconds: response.expires_in }))
      const accessToken = Model.AccessToken.make(response.token)
      const refreshToken = Model.RefreshToken.make(response.refresh_token ?? cache.refreshToken)
      const refreshedAuth = AuthStorageSchema.make({
        userId: response.user.id,
        accounts: response.user.accounts,
        accessToken,
        refreshToken,
        expiry,
      })

      // Reset the cached tokens
      yield* kvs.set(AUTH_TOKEN_CACHE_KEY, refreshedAuth)

      return refreshedAuth
    })

    const JWKS = jose.createRemoteJWKSet(new URL("/.well-known/jwks.json", AUTH_PLATFORM_URL))
    const verifyJwt = (token: string, issuer: string) =>
      Effect.tryPromise({
        try: () => jose.jwtVerify(token, JWKS, { issuer }),
        catch: (cause) => new VerifySignedAccessTokenError({ cause }),
      })

    const verifySignedAccessToken = (token: Redacted.Redacted<string>, issuer: string) =>
      verifyJwt(Redacted.value(token), issuer).pipe(
        Effect.map(({ payload }) => payload.sub ?? "sub unknown"),
      )

    // ------------------------------------------------------------------------
    // Cache Operations
    // ------------------------------------------------------------------------

    const getCache = Effect.fn("AuthService.getToken")(function*() {
      const cache = yield* Effect.flatten(kvs.get(AUTH_TOKEN_CACHE_KEY))

      const now = yield* Clock.currentTimeMillis

      // Check if we need to refresh the token
      const needsRefresh =
        // Missing expiry field - refresh to populate it
        Predicate.isNullable(cache.expiry) ||
        // Missing accounts field - refresh to populate it
        Predicate.isNullable(cache.accounts) ||
        // Token is expired
        cache.expiry < now ||
        // Token is expiring within 5 minutes
        cache.expiry - now <= 5 * 60 * 1000

      // If a refresh is required, perform the refresh request
      if (needsRefresh) {
        return yield* refreshAccessToken(cache)
      }

      // Token is still valid, return as is
      return cache
    }, Effect.option)

    const setCache = Effect.fn("AuthService.setToken")( function*(cache: AuthStorageSchema) {
        yield* kvs.set(AUTH_TOKEN_CACHE_KEY, cache)
      })

    const clearCache = kvs.remove(AUTH_TOKEN_CACHE_KEY).pipe(
      Effect.catchIf(
        (error) => error._tag === "SystemError" && error.reason === "NotFound",
        () => Effect.void
      )
    )

    return {
      generateAccessToken,
      refreshAccessToken,
      verifySignedAccessToken,
      getCache,
      setCache,
      clearCache,
    } as const
  }),
}) {}

export const layer = AuthService.Default
