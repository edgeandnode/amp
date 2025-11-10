import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Auth from "../../../Auth.ts"
import * as Model from "../../../Model.ts"
import { ExitCode } from "../../common.ts"

export const token = Command.make("token", {
  args: {
    duration: Options.text("duration").pipe(
      Options.withAlias("d"),
      Options.withDescription("Duration of the generated access token before it expires"),
      Options.withSchema(Model.GenrateTokenDuration),
      Options.withDefault("1 hour"),
    ),
    audience: Options.text("audience").pipe(
      Options.withAlias("a"),
      Options.withDescription("URLs that are valid to use the generated access token. Becomes the JWT aud value"),
      Options.repeated,
      Options.optional,
    ),
  },
}).pipe(
  Command.withDescription(
    "Generates an access token (Bearer JWT) to be used in your dapp to query a published Amp Dataset",
  ),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const auth = yield* Auth.AuthService

      const maybeAuthStorage = yield* auth.get()
      if (Option.isNone(maybeAuthStorage)) {
        yield* Console.error("Must be authenticated to generate an access token")
        yield* Console.error(`Run "amp auth login" to authenticate`)
        return yield* ExitCode.NonZero
      }
      const authStorage = maybeAuthStorage.value

      const response = yield* auth.generateAccessToken({
        storedAuth: authStorage,
        exp: args.duration,
        audience: Option.getOrElse(args.audience, () => undefined),
      }).pipe(
        Effect.catchTag("Amp/errors/auth/GenerateAccessTokenError", (error) =>
          Console.error(`Failed to generate access token: ${error.error_description} (${error.error})`).pipe(
            Effect.flatMap(() =>
              ExitCode.NonZero
            ),
          )),
        Effect.catchAll(() =>
          Console.error("Failure generating the access token. Please try again").pipe(
            Effect.flatMap(() => ExitCode.NonZero),
          )
        ),
      )

      // verify the auth token against the JWKS
      yield* auth
        .verifySignedAccessToken(
          Redacted.make(response.token),
          response.iss,
        )
        .pipe(
          Effect.catchTag("Amp/errors/auth/VerifySignedAccessTokenError", (error) =>
            Console.error(`Failed to verify the signed token. JWT incorrectly formed: ${error.message}`).pipe(
              Effect.flatMap(() =>
                ExitCode.NonZero
              ),
            )),
        )

      yield* Console.log("Access token generated.")
      yield* Console.log("We do not store this value. You will need to store it.")
      yield* Console.log(
        "Use this as a Authorization Bearer token in requests to query a published Amp Dataset to the Gateway",
      )
      yield* Console.log(`    token:`, response.token)
      yield* Console.log(`    exp:`, response.exp)
      return yield* ExitCode.Zero
    })
  ),
  Command.provide(Auth.layer),
)
