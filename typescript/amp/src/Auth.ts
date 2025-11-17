import * as Auth from "@effect/platform/FetchHttpClient"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import type * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Data from "effect/Data"
import * as DateTime from "effect/DateTime"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fn from "effect/Function"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as jose from "jose"
import * as os from "node:os"
import * as path from "node:path"
import * as Model from "./Model.ts"

export const AUTH_PLATFORM_URL = new URL("https://auth.amp.thegraph.com/")
const JWKS = jose.createRemoteJWKSet(new URL("/.well-known/jwks.json", AUTH_PLATFORM_URL))

const AuthUserId = Schema.NonEmptyTrimmedString.pipe(
  Schema.pattern(/^(c[a-z0-9]{24}|did:privy:c[a-z0-9]{24})$/),
)

export class RefreshTokenResponse extends Schema.Class<RefreshTokenResponse>("Amp/models/auth/RefreshTokenResponse")({
  token: Schema.NonEmptyTrimmedString.annotations({
    identifier: "RefreshTokenResponse.token",
    description: "The refreshed access token",
  }),
  refresh_token: Schema.NullOr(Schema.String).annotations({
    identifier: "RefreshTokenResponse.refresh_token",
  }),
  session_update_action: Schema.String.annotations({
    identifier: "RefreshTokenResponse.session_update_action",
  }),
  expires_in: Schema.Int.pipe(Schema.positive()).annotations({
    identifier: "RefreshTokenResponse.expires_in",
    description: "Seconds from receipt of when the token expires (def is 1hr)",
  }),
  user: Schema.Struct({
    id: AuthUserId,
    accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Model.Address)).annotations({
      identifier: "RefreshTokenResponse.user.accounts",
      description: "List of accounts (connected wallets, etc) belonging to the user",
      examples: [["cmfd6bf6u006vjx0b7xb2eybx", "0x5c8fA0bDf68C915a88cD68291fC7CF011C126C29"]],
    }),
  }).annotations({
    identifier: "RefreshTokenResponse.user",
    description: "The user the access token belongs to",
  }),
}) {}
export class GenerateTokenResponse extends Schema.Class<GenerateTokenResponse>(
  "Amp/models/auth/GenerateTokenResponse",
)({
  token: Schema.NonEmptyTrimmedString,
  token_type: Schema.Literal("Bearer"),
  exp: Schema.Int.pipe(Schema.positive()),
  sub: Schema.NonEmptyTrimmedString,
  iss: Schema.String,
}) {}
export class AuthStorageSchema extends Schema.Class<AuthStorageSchema>("Amp/models/auth/AuthStorageSchema")({
  accessToken: Schema.NonEmptyTrimmedString,
  refreshToken: Schema.NonEmptyTrimmedString,
  userId: AuthUserId,
  accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Model.Address)).pipe(Schema.optional),
  expiry: Schema.Int.pipe(Schema.positive(), Schema.optional),
}) {}
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
export class GenerateAccessTokenError extends Data.TaggedError("Amp/errors/auth/GenerateAccessTokenError")<{
  readonly error: string
  readonly error_description: string
  readonly status: number
}> {}
export class VerifySignedAccessTokenError extends Data.TaggedError("Amp/errors/auth/VerifySignedAccessTokenError")<{
  readonly error: string | unknown
}> {}

const AUTH_TOKEN_STORAGE_KEY = "amp_cli_auth"

export class AuthService extends Effect.Service<AuthService>()("Amp/AuthService", {
  dependencies: [
    KeyValueStore.layerFileSystem(path.join(os.homedir(), ".amp-cli-config")),
    Auth.layer,
  ],
  effect: Effect.gen(function*() {
    const httpClient = yield* HttpClient.HttpClient
    const kv = (yield* KeyValueStore.KeyValueStore).forSchema(AuthStorageSchema)

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

    const refreshAccessToken = Effect.fn("RefreshAccessToken")(function*(
      storedAuth: AuthStorageSchema,
    ) {
      const accessToken = storedAuth.accessToken
      const refreshToken = storedAuth.refreshToken
      const userId = storedAuth.userId

      const body = yield* HttpBody.json({
        refresh_token: refreshToken,
        user_id: userId,
      })
      const req = HttpClientRequest.post(`${AUTH_PLATFORM_URL}api/v1/auth/refresh`).pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bearerToken(accessToken),
        HttpClientRequest.setHeaders({
          "Content-Type": "application/json",
        }),
        HttpClientRequest.setBody(body),
      )

      const resp = yield* httpClient.execute(req).pipe(
        Effect.timeout(Duration.seconds(15)),
        // Validate successful status or handle errors
        Effect.flatMap((response) =>
          Effect.if(response.status === 200, {
            onTrue: () => Effect.succeed(response),
            onFalse: () =>
              Effect.gen(function*() {
                const errorDescription = yield* extractErrorDescription(response)

                // Handle rate limiting (429)
                if (response.status === 429) {
                  const retryAfterHeader = response.headers["retry-after"]
                  const retryAfter = retryAfterHeader ? globalThis.parseInt(retryAfterHeader, 10) : 60

                  return yield* Effect.fail(
                    new AuthRateLimitError({
                      retryAfter,
                      message: errorDescription,
                    }),
                  )
                }

                // Handle authentication failures (401/403)
                if (response.status === 401 || response.status === 403) {
                  return yield* Effect.fail(new AuthTokenExpiredError())
                }

                // Handle server errors and other failures
                return yield* Effect.fail(
                  new AuthRefreshError({
                    status: response.status,
                    message: errorDescription,
                  }),
                )
              }),
          })
        ),
      )

      const tokenResponse = yield* resp.json.pipe(Effect.flatMap(Schema.decodeUnknown(RefreshTokenResponse)))

      // Validate that the response user ID matches the stored user ID
      if (tokenResponse.user.id !== storedAuth.userId) {
        return yield* Effect.fail(
          new AuthUserMismatchError({
            expected: storedAuth.userId,
            received: tokenResponse.user.id,
          }),
        )
      }

      const now = yield* DateTime.now
      const expiry = Fn.pipe(now, DateTime.add({ seconds: tokenResponse.expires_in }), DateTime.toEpochMillis)
      const refreshedAuth = AuthStorageSchema.make({
        accessToken: tokenResponse.token,
        refreshToken: tokenResponse.refresh_token ?? refreshToken,
        userId: tokenResponse.user.id,
        accounts: tokenResponse.user.accounts,
        expiry,
      })
      yield* kv.set(AUTH_TOKEN_STORAGE_KEY, refreshedAuth)

      return refreshedAuth
    })

    const generateAccessToken = Effect.fn("GenerateAccessToken")(function*(
      args: Readonly<{
        storedAuth: AuthStorageSchema
        exp: Model.GenrateTokenDuration | undefined
        audience: ReadonlyArray<string> | null | undefined
      }>,
    ) {
      const accessToken = args.storedAuth.accessToken
      const body = yield* HttpBody.jsonSchema(
        Schema.Struct({
          duration: Schema.Union(
            Schema.Number,
            Schema.DateFromSelf,
            Schema.String.pipe(
              Schema.pattern(
                /^-?\d+\.?\d*\s*(sec|secs|second|seconds|s|minute|minutes|min|mins|m|hour|hours|hr|hrs|h|day|days|d|week|weeks|w|year|years|yr|yrs|y)(\s+ago|\s+from\s+now)?$/i,
              ),
            ),
          ).pipe(Schema.optionalWith({ nullable: true })),
          audience: Schema.Array(Schema.String).pipe(Schema.optionalWith({ nullable: true })),
        }),
      )({
        duration: args.exp || undefined,
        audience: args.audience || undefined,
      })
      const req = HttpClientRequest.post(new URL("/api/v1/auth/generate", AUTH_PLATFORM_URL), {
        acceptJson: true,
        headers: {
          "Content-Type": "application/json",
        },
      }).pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bearerToken(accessToken),
        HttpClientRequest.setBody(body),
      )

      const resp = yield* httpClient.execute(req)
      if (resp.status !== 200) {
        // Parse error response from API, with fallback for non-JSON responses
        const errorResponse = yield* resp.json.pipe(Effect.catchAll(() => Effect.succeed({})))
        const error = typeof errorResponse === "object" && errorResponse !== null && "error" in errorResponse
          ? String(errorResponse.error)
          : "server_error"
        const error_description =
          typeof errorResponse === "object" && errorResponse !== null && "error_description" in errorResponse
            ? String(errorResponse.error_description)
            : "Failed to generate access token"

        return yield* Effect.fail(
          new GenerateAccessTokenError({
            error,
            error_description,
            status: resp.status,
          }),
        )
      }

      return yield* resp.json.pipe(Effect.flatMap(Schema.decodeUnknown(GenerateTokenResponse)))
    })

    const verifySignedAccessToken = (token: Redacted.Redacted<string>, issuer: string) =>
      Effect.tryPromise({
        async try() {
          const { payload } = await jose.jwtVerify(Redacted.value(token), JWKS, {
            issuer,
          })

          return payload.sub || "sub unknown"
        },
        catch(error) {
          return new VerifySignedAccessTokenError({ error })
        },
      })

    const maybeGetToken = kv.get(AUTH_TOKEN_STORAGE_KEY)

    const get = Effect.fn("FetchAuthToken")(function*() {
      return yield* maybeGetToken.pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed(Option.none<AuthStorageSchema>()),
            onSome: (token) =>
              Effect.gen(function*() {
                // Check if we need to refresh the token
                const needsRefresh =
                  // Missing expiry field - refresh to populate it
                  token.expiry == null ||
                  // Missing accounts field - refresh to populate it
                  token.accounts == null ||
                  // Token is expired
                  token.expiry < Date.now() ||
                  // Token is expiring within 5 minutes
                  token.expiry - Date.now() <= 5 * 60 * 1000

                if (needsRefresh) {
                  return yield* refreshAccessToken(token).pipe(Effect.map(Option.some))
                }

                // Token is still valid, return it as-is
                return Option.some(token)
              }),
          }),
        ),
        // Return None on any error during refresh
        Effect.orElse(() => Effect.succeed(Option.none<AuthStorageSchema>())),
      )
    })

    return {
      refreshAccessToken,
      get,
      set: (data: AuthStorageSchema) => kv.set(AUTH_TOKEN_STORAGE_KEY, data),
      generateAccessToken,
      verifySignedAccessToken,
      delete: kv.remove(AUTH_TOKEN_STORAGE_KEY).pipe(
        Effect.catchIf(
          (error): error is Extract<typeof error, { _tag: "SystemError"; reason: "NotFound" }> =>
            error._tag === "SystemError" && error.reason === "NotFound",
          () => Effect.void,
        ),
      ),
    } as const
  }),
}) {}
export const layer = AuthService.Default
