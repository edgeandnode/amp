import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiMiddleware from "@effect/platform/HttpApiMiddleware"
import * as HttpApiSecurity from "@effect/platform/HttpApiSecurity"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import { type AuthTokenClaims, PrivyClient, type User } from "@privy-io/server-auth"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as os from "node:os"
import * as path from "node:path"

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
  user: Schema.Struct({
    id: Schema.String,
  }).annotations({
    identifier: "RefreshTokenResponse.user",
    description: "The user the access token belongs to",
  }),
}) {}
export class RefreshAccessToken extends Schema.Class<RefreshAccessToken>("Amp/models/auth/RefreshAccessToken")({
  accessToken: Schema.Redacted(Schema.NonEmptyTrimmedString),
  refreshToken: Schema.Redacted(Schema.NonEmptyTrimmedString),
}) {}
export class AuthPayload extends Schema.Class<AuthPayload>("Amp/models/auth/AuthPayload")({
  userId: Schema.NonEmptyTrimmedString,
  expiration: Schema.NonNegativeInt,
  issuedAt: Schema.NonNegativeInt,
  sessionId: Schema.String,
  token: Schema.NonEmptyTrimmedString,
}) {}
export class AuthStorageSchema extends Schema.Class<AuthStorageSchema>("Amp/models/auth/AuthStorageSchema")({
  accessToken: Schema.NonEmptyTrimmedString,
  refreshToken: Schema.NonEmptyTrimmedString,
  userId: Schema.NonEmptyTrimmedString,
}) {}

export class AuthTokenInvalidError
  extends Schema.TaggedError<AuthTokenInvalidError>("Amp/errors/auth/AuthTokenInvalidError")("AuthTokenInvalidError", {
    cause: Schema.Unknown,
  })
{}
export class UserNotFoundError
  extends Schema.TaggedError<UserNotFoundError>("Amp/errors/auth/UserNotFoundError")("UserNotFoundError", {
    id: Schema.NonEmptyTrimmedString,
    cause: Schema.Unknown,
  })
{}
export class AuthTokenExpiredError
  extends Schema.TaggedError<AuthTokenExpiredError>("Amp/errors/auth/AuthTokenExpiredError")(
    "AuthTokenExpiredError",
    {},
  )
{}
export class AuthTokenNotFoundError
  extends Schema.TaggedError<AuthTokenNotFoundError>("Amp/errors/auth/AuthTokenNotFoundError")(
    "AuthTokenNotFoundError",
    {},
  )
{}

const AUTH_APP_ID = Schema.Config("AUTH_APP_ID", Schema.Redacted(Schema.String))
const AUTH_APP_SECRET = Schema.Config("AUTH_APP_SECRET", Schema.Redacted(Schema.String))

const AUTH_TOKEN_STORAGE_KEY = "amp_cli_auth"
const DEFAULT_AUTH_ORIGIN = new URL("http://localhost:3001")

export class AuthService extends Effect.Service<AuthService>()("Amp/AuthService", {
  dependencies: [
    FetchHttpClient.layer,
    KeyValueStore.layerFileSystem(path.join(os.homedir(), ".amp-cli-config")),
  ],
  effect: Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient
    const kv = (yield* KeyValueStore.KeyValueStore).forSchema(AuthStorageSchema)

    const [appId, appSecret] = yield* Effect.all([
      AUTH_APP_ID.pipe(Effect.map(Redacted.value)),
      AUTH_APP_SECRET.pipe(Effect.map(Redacted.value)),
    ])

    const authClient = new PrivyClient(appId, appSecret)

    const getUserById = (id: string) =>
      Effect.tryPromise({
        async try() {
          return await authClient.getUserById(id)
        },
        catch(error) {
          return new UserNotFoundError({ id, cause: error })
        },
      })
    const getUserByIdCache = Effect.fn("GetUserById")(function*(id: string) {
      return yield* Effect.withRequestCaching(true)(getUserById(id))
    })

    const verifyAuthToken = (token: Redacted.Redacted<string>) =>
      Effect.tryPromise({
        async try() {
          const accessToken = Redacted.value(token)
          const claims = await authClient.verifyAuthToken(accessToken)

          return AuthPayload.make({
            userId: claims.userId,
            expiration: claims.expiration,
            issuedAt: claims.issuedAt,
            sessionId: claims.sessionId,
            token: accessToken,
          })
        },
        catch(error) {
          return new AuthTokenInvalidError({ cause: error })
        },
      })

    const refreshAccessToken = Effect.fn("RefreshAccessToken")(function*(
      storedAuth: AuthStorageSchema,
      origin: URL | string = DEFAULT_AUTH_ORIGIN,
    ) {
      const _accessToken = storedAuth.accessToken
      const _refreshToken = storedAuth.refreshToken

      const body = yield* HttpBody.json({
        refresh_token: _refreshToken,
      })
      const req = HttpClientRequest.post("https://auth.privy.io/api/v1/sessions").pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bearerToken(_accessToken),
        HttpClientRequest.setHeaders({
          "Privy-App-Id": appId,
          "Content-Type": "application/json",
          Origin: typeof origin === "string" ? origin : origin.href,
        }),
        HttpClientRequest.setBody(body),
      )

      const resp = yield* client.execute(req).pipe(
        Effect.filterOrFail(
          (response) => response.status === 200,
          () => new AuthTokenExpiredError(),
        ),
      )

      const tokenResponse = yield* resp.json.pipe(Effect.flatMap(Schema.decodeUnknown(RefreshTokenResponse)))

      const refreshedAuth = AuthStorageSchema.make({
        accessToken: tokenResponse.token,
        refreshToken: tokenResponse.refresh_token ?? _refreshToken,
        userId: storedAuth.userId,
      })

      // Update the KV store with the refreshed tokens
      yield* kv.set(AUTH_TOKEN_STORAGE_KEY, refreshedAuth)

      return refreshedAuth
    })

    const maybeGetToken = kv.get(AUTH_TOKEN_STORAGE_KEY)

    const get = Effect.fn("FetchAuthToken")(function*(origin: URL = DEFAULT_AUTH_ORIGIN) {
      return yield* maybeGetToken.pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed(Option.none<AuthStorageSchema>()),
            onSome: (token) => refreshAccessToken(token, origin).pipe(Effect.map(Option.some)),
          }),
        ),
        Effect.orElseSucceed(() => Option.none<AuthStorageSchema>()),
      )
    })
    const getRequired = Effect.fn("FetchAuthTokenRequired")(function*(origin: URL = DEFAULT_AUTH_ORIGIN) {
      const tokenOpt = yield* maybeGetToken.pipe(
        Effect.tapErrorCause((cause) => Effect.logError("Failure fetching the auth token", Cause.pretty(cause))),
        Effect.mapError(() => new AuthTokenNotFoundError()),
      )

      return yield* Option.match(tokenOpt, {
        onNone: () => Effect.fail(new AuthTokenNotFoundError()),
        onSome: (token) => refreshAccessToken(token, origin),
      })
    })

    return {
      getUserById: getUserByIdCache,
      verifyAuthToken,
      refreshAccessToken,
      get,
      getRequired,
      set: (data: AuthStorageSchema) => kv.set(AUTH_TOKEN_STORAGE_KEY, data),
      delete: kv.remove(AUTH_TOKEN_STORAGE_KEY).pipe(
        Effect.catchTag("SystemError", (sysErr) => {
          if (sysErr.reason === "NotFound") {
            // file not found, succeed since same as removing
            return Effect.void
          }
          return Effect.fail(sysErr)
        }),
      ),
      fetchUserDisplayName: Effect.fn("FetchAuthenticatedUserDisplayName")(function*(
        userIdOrClaims: string | AuthTokenClaims | AuthPayload,
      ) {
        const userId = typeof userIdOrClaims === "string" ? userIdOrClaims : userIdOrClaims.userId
        return yield* getUserByIdCache(userId).pipe(Effect.map((user) => getUserDisplayName(user)[1]))
      }),
      // Util function to wrap fetching the auth state from the KV store,
      // and if present:
      // - refreshes the access token
      // - updates the KV store with refreshed tokens
      // - fetches the display name using the stored userId
      getWithDisplayName: Effect.fn("FetchAuthTokenWithDisplayName")(function*(
        origin: URL = DEFAULT_AUTH_ORIGIN,
      ) {
        const authOpt = yield* get(origin)
        if (Option.isNone(authOpt)) {
          return Option.none<string>()
        }
        const displayName = yield* getUserByIdCache(authOpt.value.userId).pipe(
          Effect.map((user) => getUserDisplayName(user)[1]),
        )
        return Option.some(displayName)
      }),
    } as const
  }),
}) {}
export const layer = AuthService.Default

/**
 * Represents the derived AuthPayload for the _current_ request.
 * Passed along in the context of the api.
 */
export class CurrentAuthPayload
  extends Context.Tag("Amp/models/auth/CurrentAuthPayload")<CurrentAuthPayload, AuthPayload>()
{}

export class AuthenticationMiddleware extends HttpApiMiddleware.Tag<AuthenticationMiddleware>()(
  "Amp/auth/middle/AuthenticationMiddleware",
  {
    failure: HttpApiError.Unauthorized,
    provides: CurrentAuthPayload,
    security: {
      // auth will be a JWT provided on the Authorization header as a bearer token
      // ex: -H 'Authorization: Bearer abc123'
      bearer: HttpApiSecurity.bearer,
    },
  },
) {}
export const AuthenticationMiddlewareLive = Layer.effect(
  AuthenticationMiddleware,
  Effect.gen(function*() {
    const auth = yield* AuthService

    return AuthenticationMiddleware.of({
      bearer(token) {
        const accessToken = Redacted.value(token)
        return auth.verifyAuthToken(token).pipe(
          Effect.tapBoth({
            onFailure: (e) => Effect.logError("failure verifying auth token", { e }),
            onSuccess: () => Effect.logDebug("access token verified"),
          }),
          Effect.map((claims) => AuthPayload.make({ ...claims, token: accessToken })),
          // If payload is None, fail with Unauthorized error, otherwise extract the value
          Effect.mapError(() => new HttpApiError.Unauthorized()),
        )
      },
    })
  }),
).pipe(Layer.provide(layer))

type AuthType = "wallet" | "github" | "discord" | "email" | "google" | "farcaster" | "unknown"
function getUserDisplayName(user: User): [type: AuthType, display: string, full: string] {
  if (user.discord?.username != null) {
    return ["discord", user.discord.username, user.discord.username] as const
  } else if (user.github?.username != null) {
    return ["github", user.github.username, user.github.username] as const
  } else if (user.email?.address) {
    return ["email", user.email.address, user.email.address] as const
  } else if (user.google?.email) {
    return ["google", user.google.email, user.google.email] as const
  } else if (user.farcaster?.displayName || user.farcaster?.username || user.farcaster?.ownerAddress) {
    if (user.farcaster.displayName) {
      return ["farcaster", user.farcaster.displayName, user.farcaster.displayName] as const
    } else if (user.farcaster.username) {
      return ["farcaster", user.farcaster.username, user.farcaster.username] as const
    }
    return ["farcaster", shorten(user.farcaster.ownerAddress), user.farcaster.ownerAddress] as const
  } else if (user.wallet?.address != null) {
    return ["wallet", shorten(user.wallet.address), user.wallet.address] as const
  } else if (user.smartWallet?.address != null) {
    return ["wallet", shorten(user.smartWallet.address), user.smartWallet.address] as const
  }
  return ["unknown", user.id, user.id] as const
}
function shorten(val: string): string {
  if (val.length <= 18) {
    return val
  }
  return `${val.substring(0, 6)}...${val.substring(val.length - 6, val.length)}`
}
