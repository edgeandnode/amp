import * as Command from "@effect/cli/Command"
import * as Prompt from "@effect/cli/Prompt"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as Cause from "effect/Cause"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Auth from "../../../Auth.ts"

const confirm = Prompt.confirm({
  message: "Are you sure you like to logout?",
  initial: true,
})

export const logout = Command.prompt("logout", Prompt.all([confirm]), ([confirm]) =>
  Effect.gen(function*() {
    const auth = yield* Auth.AuthService

    if (!confirm) {
      return yield* Console.log("Exiting...")
    }

    return yield* auth.clearCache.pipe(
      Effect.tapErrorCause((cause) =>
        Console.debug("Failure removing the auth token from the KV Store", Cause.pretty(cause))
      ),
      Effect.tap(() => Console.log("You have successfully logged out.")),
    )
  })).pipe(
    Command.withDescription("Logs the authenticated user out of the cli"),
    Command.provide(Layer.mergeAll(Auth.layer, NodeFileSystem.layer, NodePath.layer)),
  )
