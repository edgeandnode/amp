import * as Command from "@effect/cli/Command"
import * as Prompt from "@effect/cli/Prompt"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Auth from "../../../Auth.ts"

const confirm = Prompt.confirm({
  message: "Are you sure you like to logout?",
  initial: true,
})

export const logout = Command.prompt("logout", Prompt.all([confirm]), ([confirm]) =>
  Effect.gen(function*() {
    const auth = yield* Auth.AuthService

    if (!confirm) {
      return yield* Effect.logInfo("Exiting...")
    }

    return yield* auth.delete.pipe(
      Effect.tapErrorCause((cause) =>
        Effect.logDebug("Failure removing the auth token from the KV Store", Cause.pretty(cause))
      ),
      Effect.tap(() => Effect.logInfo("You have successfully logged out.")),
    )
  })).pipe(
    Command.withDescription("Logs the authenticated user out of the cli"),
    Command.provide(Auth.layer),
    Command.provide(NodeFileSystem.layer),
    Command.provide(NodePath.layer),
  )
