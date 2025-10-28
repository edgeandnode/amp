import * as Command from "@effect/cli/Command"
import * as Prompt from "@effect/cli/Prompt"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as Config from "effect/Config"
import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as crypto from "node:crypto"
import open from "open"
import * as Auth from "../../../Auth.ts"

/**
 * Base64 URL-safe encoding (RFC 4648)
 */
function base64UrlEncode(buffer: Buffer): string {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "")
}

/**
 * Generate a cryptographically random code_verifier
 * Must be 43-128 characters, using unreserved characters [A-Za-z0-9-._~]
 */
const generateCodeVerifier = Effect.sync(() => {
  const randomBytes = crypto.randomBytes(32)
  return base64UrlEncode(randomBytes)
})

/**
 * Generate code_challenge from code_verifier
 * code_challenge = BASE64URL(SHA256(code_verifier))
 */
const generateCodeChallenge = (verifier: string) =>
  Effect.sync(() => {
    const hash = crypto.createHash("sha256").update(verifier).digest()
    return base64UrlEncode(hash)
  })

const AUTH_PLATFORM_URL = Schema.Config("AUTH_PLATFORM_URL", Schema.URL).pipe(
  Config.withDefault(new URL("https://platform-auth-ui.vercel.app/")),
)

// Branded types to prevent mixing up device codes and user codes
const DeviceCode = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("DeviceCode"),
)
type DeviceCode = Schema.Schema.Type<typeof DeviceCode>

const UserCode = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("UserCode"),
)
type UserCode = Schema.Schema.Type<typeof UserCode>

class DeviceAuthorizationResponse
  extends Schema.Class<DeviceAuthorizationResponse>("Amp/auth/login/models/DeviceAuthorizationResponse")({
    device_code: DeviceCode.annotations({
      identifier: "DeviceAuthorizationResponse.device_code",
      description: "Device verification code used for polling",
    }),
    user_code: UserCode.annotations({
      identifier: "DeviceAuthorizationResponse.user_code",
      description: "User code to display for manual entry",
    }),
    verification_uri: Schema.String.annotations({
      identifier: "DeviceAuthorizationResponse.verification_uri",
      description: "URL where user enters the code",
    }),
    expires_in: Schema.Int.pipe(Schema.positive()).annotations({
      identifier: "DeviceAuthorizationResponse.expires_in",
      description: "Time in seconds until device code expires",
    }),
    interval: Schema.Int.pipe(Schema.positive()).annotations({
      identifier: "DeviceAuthorizationResponse.interval",
      description: "Minimum polling interval in seconds",
    }),
  })
{}

class DeviceTokenResponse extends Schema.Class<DeviceTokenResponse>("Amp/auth/login/models/DeviceTokenResponse")({
  access_token: Schema.NonEmptyTrimmedString.annotations({
    identifier: "DeviceTokenResponse.access_token",
    description: "The access token for authenticated requests",
  }),
  refresh_token: Schema.NonEmptyTrimmedString.annotations({
    identifier: "DeviceTokenResponse.refresh_token",
    description: "The refresh token for renewing access",
  }),
  user_id: Schema.NonEmptyTrimmedString.annotations({
    identifier: "DeviceTokenResponse.user_id",
    description: "The authenticated user's ID",
  }),
}) {}

class DeviceTokenPendingResponse
  extends Schema.Class<DeviceTokenPendingResponse>("Amp/auth/login/models/DeviceTokenPendingResponse")({
    error: Schema.Literal("authorization_pending"),
  })
{}

class DeviceTokenExpiredResponse
  extends Schema.Class<DeviceTokenExpiredResponse>("Amp/auth/login/models/DeviceTokenExpiredResponse")({
    error: Schema.Literal("expired_token"),
  })
{}

const DeviceTokenPollingResponse = Schema.Union(
  DeviceTokenResponse,
  DeviceTokenPendingResponse,
  DeviceTokenExpiredResponse,
)

const checkAlreadyAuthenticated = Effect.gen(function*() {
  const auth = yield* Auth.AuthService

  const authResult = yield* auth.get()
  return Option.isSome(authResult)
}).pipe(
  // if any error occurs, return false and make the user authenticate
  Effect.orElseSucceed(() => false),
)

export const login = Command.make("login").pipe(
  Command.withDescription("Performs the login flow for authenticating the cli"),
  Command.withHandler(handleCommand),
  Command.provide(Auth.layer),
  Command.provide(FetchHttpClient.layer),
  Command.provide(NodeFileSystem.layer),
  Command.provide(NodePath.layer),
)

function handleCommand() {
  return resolveAuthToken().pipe(
    Effect.flatMap(authenticate),
  )
}

const spinnerFrames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
const showLoadingSpinner = (message: string) =>
  Stream.fromSchedule(Schedule.fixed(Duration.millis(80))).pipe(
    Stream.scan(0, (index) => (index + 1) % spinnerFrames.length),
    Stream.map((index) => {
      const spinner = spinnerFrames[index]
      return `\x1b[36m${spinner}\x1b[0m ${message}`
    }),
    Stream.tap((rendered) =>
      Effect.sync(() => {
        process.stdout.write(`\r${rendered}`)
      })
    ),
  )

const requestDeviceAuthorization = Effect.fn("RequestDeviceAuthorization")(function*() {
  const authPlatformUrl = yield* AUTH_PLATFORM_URL
  const client = yield* HttpClient.HttpClient

  // Generate PKCE parameters
  const codeVerifier = yield* generateCodeVerifier
  const codeChallenge = yield* generateCodeChallenge(codeVerifier)

  const deviceAuthUrl = new URL("/api/v1/device/authorize", authPlatformUrl)

  const request = yield* HttpClientRequest.post(deviceAuthUrl.toString()).pipe(
    HttpClientRequest.acceptJson,
    HttpClientRequest.bodyJson({
      code_challenge: codeChallenge,
      code_challenge_method: "S256",
    }),
  )

  const response = yield* client.execute(request).pipe(
    Effect.timeout(Duration.seconds(30)),
    Effect.flatMap((res) =>
      res.status === 200
        ? Effect.succeed(res)
        : Effect.flatMap(res.text, (body) =>
          Effect.fail(
            new DeviceAuthorizationError({
              url: deviceAuthUrl.toString(),
              status: res.status,
              body,
              cause: undefined,
            }),
          ))
    ),
  )

  const json = yield* response.json
  const deviceAuth = yield* Schema.decodeUnknown(DeviceAuthorizationResponse)(json).pipe(
    Effect.mapError((error) =>
      new DeviceAuthorizationError({
        url: deviceAuthUrl.toString(),
        cause: error,
      })
    ),
  )

  return { deviceAuth, codeVerifier }
})

const pollForToken = Effect.fn("PollForDeviceToken")(function*(deviceCode: DeviceCode, codeVerifier: string) {
  const authPlatformUrl = yield* AUTH_PLATFORM_URL
  const client = yield* HttpClient.HttpClient

  const tokenUrl = new URL("/api/v1/device/token", authPlatformUrl)
  tokenUrl.searchParams.set("device_code", deviceCode)
  tokenUrl.searchParams.set("code_verifier", codeVerifier)

  const request = HttpClientRequest.get(tokenUrl.toString()).pipe(
    HttpClientRequest.acceptJson,
  )

  const response = yield* client.execute(request).pipe(
    Effect.timeout(Duration.seconds(10)),
  )

  yield* Effect.logDebug(`Polling response status: ${response.status}`)

  const json = yield* response.json.pipe(
    Effect.timeout(Duration.seconds(40)),
  )
  yield* Effect.logDebug(`Polling response body:`, json)

  const result = yield* Schema.decodeUnknown(DeviceTokenPollingResponse)(json).pipe(
    Effect.tapError((error) =>
      Effect.logError(`Failed to decode polling response:`, {
        status: response.status,
        json,
        error,
      })
    ),
    Effect.mapError((error) =>
      new DeviceTokenPollingError({
        url: tokenUrl.toString(),
        status: response.status,
        cause: error,
      })
    ),
  )

  if (result instanceof DeviceTokenPendingResponse) {
    yield* Effect.logDebug("Token still pending, retrying...")
    return yield* Effect.fail(new DeviceTokenStillPendingError())
  }

  if (result instanceof DeviceTokenExpiredResponse) {
    yield* Effect.logError("Device token expired")
    return yield* Effect.fail(new DeviceTokenExpiredError())
  }

  yield* Effect.logDebug("Token received successfully!")
  return result
})

const displayUserCodeAndOpenBrowser = Effect.fn("DisplayUserCodeAndOpenBrowser")(function*(
  deviceAuth: DeviceAuthorizationResponse,
) {
  yield* Effect.logInfo(`Enter this verification code in your browser: ${deviceAuth.user_code}`)

  const verificationUrl = new URL(deviceAuth.verification_uri)

  yield* openBrowser(verificationUrl.toString()).pipe(
    Effect.tapError(() =>
      Effect.logInfo(
        `If browser does not open automatically, go to ${verificationUrl.toString()} and enter code: ${deviceAuth.user_code}`,
      )
    ),
  )
})

const pollUntilAuthenticated = Effect.fn("PollUntilAuthenticated")(function*(
  deviceCode: DeviceCode,
  codeVerifier: string,
  interval: number,
  expiresIn: number,
) {
  // Start with faster exponential backoff (1s, 1.5s, 2.25s...), but cap at server's requested interval
  const pollingSchedule = Schedule.exponential(Duration.seconds(1), 1.5).pipe(
    Schedule.union(Schedule.spaced(Duration.seconds(interval))), // Cap at server interval
    Schedule.intersect(Schedule.recurs(Math.floor(expiresIn / interval))),
  )

  // Start spinner in background
  const spinnerFiber = yield* showLoadingSpinner("Waiting for authentication to complete...").pipe(
    Stream.runDrain,
    Effect.fork,
  )

  // Run polling with error handling
  const result = yield* pollForToken(deviceCode, codeVerifier).pipe(
    Effect.retry(
      pollingSchedule.pipe(
        Schedule.whileInput((error: unknown) => error instanceof DeviceTokenStillPendingError),
      ),
    ),
    Effect.tapError(() => Effect.logError("Authentication timed out or expired. Please try again")),
  )

  // Clean up spinner
  yield* Fiber.interrupt(spinnerFiber)
  yield* Effect.sync(() => process.stdout.write("\r\x1b[K")) // Clear the spinner line

  return result
})

const authenticate = Effect.fn("PerformCliAuthentication")(function*(alreadyAuthenticated: boolean) {
  // If we have a token, user is already authenticated
  if (alreadyAuthenticated) {
    return yield* Effect.logInfo("amp cli already authenticated!")
  }

  const auth = yield* Auth.AuthService

  // Step 1: Request device authorization from the backend
  const { codeVerifier, deviceAuth } = yield* requestDeviceAuthorization()

  // Step 2: Display the user code and open browser
  yield* displayUserCodeAndOpenBrowser(deviceAuth)

  // Step 3: Poll for token with retry logic
  const tokenResponse = yield* pollUntilAuthenticated(
    deviceAuth.device_code,
    codeVerifier,
    deviceAuth.interval,
    deviceAuth.expires_in,
  )

  // Step 4: Store the tokens
  yield* auth.set({
    accessToken: tokenResponse.access_token,
    refreshToken: tokenResponse.refresh_token,
    userId: tokenResponse.user_id,
  })

  // Step 5: Log success
  yield* Effect.logInfo("Successfully authenticated!")
})

/**
 * Resolves the token from:
 * 1. The token being pulled from the KeyValueStore (AuthService)
 * 2. The user being prompted to open their browser to authenticate
 *
 * @returns boolean indicating if already authenticated (true) or needs to authenticate (false)
 */
const resolveAuthToken = Effect.fn("ResolveAuthToken")(function*() {
  // check if the cli is already authenticated
  const alreadyAuthenticated = yield* checkAlreadyAuthenticated
  if (alreadyAuthenticated) {
    return true
  }

  // user is not already authenticated - ask if they want to open browser
  const shouldOpenBrowser = yield* Prompt.confirm({
    message: "Open browser to authenticate?",
    initial: true,
  })

  if (!shouldOpenBrowser) {
    return yield* Effect.fail(new AuthenticationCancelledError())
  }

  return false
})

const openBrowser = (url: string) =>
  Effect.async<void, OpenBrowserError>((resume) => {
    open(url)
      .then((subprocess) => {
        subprocess.on("spawn", () => resume(Effect.void))
        subprocess.on("error", (err) => resume(Effect.fail(new OpenBrowserError({ url, cause: err }))))
      })
      .catch((err) => resume(Effect.fail(new OpenBrowserError({ url, cause: err }))))
  })

export class OpenBrowserError extends Data.TaggedError("Amp/cli/auth/errors/OpenBrowserError")<{
  readonly url: string
  readonly cause: unknown
}> {}

export class AuthenticationCancelledError extends Data.TaggedError("Amp/cli/auth/errors/AuthenticationCancelled") {}

export class DeviceAuthorizationError extends Data.TaggedError("Amp/cli/auth/errors/DeviceAuthorizationError")<{
  readonly url: string
  readonly status?: number
  readonly body?: string
  readonly cause: unknown
}> {
  override get message(): string {
    if (this.body) {
      return `Device authorization error: ${this.body}`
    }
    return this.message
  }
}

export class DeviceTokenPollingError extends Data.TaggedError("Amp/cli/auth/errors/DeviceTokenPollingError")<{
  readonly url: string
  readonly status: number
  readonly cause: unknown
}> {}

export class DeviceTokenStillPendingError
  extends Data.TaggedError("Amp/cli/auth/errors/DeviceTokenStillPendingError")
{}

export class DeviceTokenExpiredError extends Data.TaggedError("Amp/cli/auth/errors/DeviceTokenExpiredError") {}
