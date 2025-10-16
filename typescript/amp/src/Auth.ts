import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiMiddleware from "@effect/platform/HttpApiMiddleware"
import * as HttpApiSecurity from "@effect/platform/HttpApiSecurity"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import { type AuthTokenClaims, PrivyClient, type User } from "@privy-io/server-auth"
import * as Cause from "effect/Cause"
import * as Config from "effect/Config"
import type * as ConfigError from "effect/ConfigError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type { ParseError } from "effect/ParseResult"
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

export const TypeId: unique symbol = Symbol.for("@edgeandnode/amp/PrivyClientService")
export type TypeId = typeof TypeId

export interface PrivyClientServiceConfig {
  appId: Redacted.Redacted<string>
  appSecret: Redacted.Redacted<string>
}
export class PrivyClientService extends Context.Tag("Amp/PrivyClientService")<PrivyClientService, {
  readonly [TypeId]: TypeId
  readonly client: PrivyClient
  /**
   * Uses the IDaas client to verify the access token is valid and not expired, etc.
   * @param token the redacted access token
   * @throws {AuthTokenInvalidError} thrown if the verifyAuthToken call throws an error
   */
  readonly verifyAuthToken: (token: Redacted.Redacted<string>) => Effect.Effect<AuthPayload, AuthTokenInvalidError>
  /**
   * Refresh the users access token within the lifespan of the refresh token.
   * This fn allows us to have safe, short-lived access tokens with refresh tokens and refresh the access tokens as needed.
   * @param accessToken the current, even if expired, access token
   * @param refreshToken the current refresh token
   * @param origin the origin of the auth platform @default http://localhost:3001
   * @returns The refreshed access token and refresh token
   * @throws {AuthTokenExpiredError} thrown if the refresh token has expired and can therefore not be used to refresh the access token
   * @throws {HttpClientError.HttpClientError} thrown if the POST request to refresh the access token fails
   * @throws {HttpBody.HttpBodyError} thrown if parsing the request body fails
   * @throws {ParseError} thrown if decoding the JSON response from the refresh token call fails
   */
  readonly refreshAccessToken: (storedAuth: AuthStorageSchema, origin?: string | URL | undefined) => Effect.Effect<
    AuthStorageSchema,
    AuthTokenExpiredError | HttpClientError.HttpClientError | HttpBody.HttpBodyError | ParseError
  >
  /**
   * Fetch the user by their id from the IDaaS
   * @param userId IDaaS id of the user to fetch
   * @returns the found User
   * @throws {UserNotFoundError} if no user exists in the IDaaS with the given id
   */
  readonly fetchUser: (userId: string) => Effect.Effect<User, UserNotFoundError>
  /**
   * Fetch the user by their id from the IDaaS.
   * Parse the display name based on their connected account type
   * @param userIdOrClaims the user id/claims (which contains the user id) to fetch the display name for
   * @returns the authenticated user display name
   * @throws {UserNotFoundError} thrown if no user found by the id from the IDaaS
   */
  readonly fetchUserDisplayName: (
    userIdOrClaims: string | AuthPayload | AuthTokenClaims,
  ) => Effect.Effect<string, UserNotFoundError, never>
}>() {}
export function makePrivyClientService(config: PrivyClientServiceConfig) {
  return Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient

    const appId = Redacted.value(config.appId)
    const appSecret = Redacted.value(config.appSecret)

    const privyClient = new PrivyClient(appId, appSecret)

    const fetchUser = (id: string) =>
      Effect.tryPromise({
        async try() {
          return await privyClient.getUserById(id)
        },
        catch(error) {
          return new UserNotFoundError({ id, cause: error })
        },
      })

    const verifyAuthToken = (token: Redacted.Redacted<string>) =>
      Effect.tryPromise({
        async try() {
          const accessToken = Redacted.value(token)
          const claims = await privyClient.verifyAuthToken(accessToken)

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

      return refreshedAuth
    })

    return {
      [TypeId]: TypeId,
      client: privyClient,
      verifyAuthToken,
      refreshAccessToken,
      fetchUser,
      fetchUserDisplayName: Effect.fn("FetchAuthenticatedUserDisplayName")(function*(
        userIdOrClaims: string | AuthTokenClaims | AuthPayload,
      ) {
        const userId = typeof userIdOrClaims === "string" ? userIdOrClaims : userIdOrClaims.userId
        return yield* fetchUser(userId).pipe(Effect.map((user) => getUserDisplayName(user)[1]))
      }),
    } as const
  })
}
export function privyClientLayerConfig(
  config: Config.Config.Wrap<PrivyClientServiceConfig>,
): Layer.Layer<PrivyClientService, ConfigError.ConfigError, HttpClient.HttpClient> {
  return Layer.scopedContext(
    Config.unwrap(config).pipe(
      Effect.flatMap(makePrivyClientService),
      Effect.map((client) => Context.make(PrivyClientService, client)),
    ),
  )
}
const PrivyClientServiceLive = privyClientLayerConfig({
  appId: AUTH_APP_ID,
  appSecret: AUTH_APP_SECRET,
}).pipe(
  Layer.provide(FetchHttpClient.layer),
)

export class AuthService extends Effect.Service<AuthService>()("Amp/AuthService", {
  dependencies: [
    KeyValueStore.layerFileSystem(path.join(os.homedir(), ".amp-cli-config")),
    PrivyClientServiceLive,
  ],
  effect: Effect.gen(function*() {
    const privyClient = yield* PrivyClientService
    const kv = (yield* KeyValueStore.KeyValueStore).forSchema(AuthStorageSchema)

    const refreshAccessToken = (
      storedAuth: AuthStorageSchema,
      origin: URL | string = DEFAULT_AUTH_ORIGIN,
    ) =>
      privyClient.refreshAccessToken(storedAuth, origin).pipe(
        Effect.tap((refreshedAuth) => kv.set(AUTH_TOKEN_STORAGE_KEY, refreshedAuth)),
      )

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
      getUserById: privyClient.fetchUser,
      verifyAuthToken: privyClient.verifyAuthToken,
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
      fetchUserDisplayName: privyClient.fetchUserDisplayName,
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
        const displayName = yield* privyClient.fetchUser(authOpt.value.userId).pipe(
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
