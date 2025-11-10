import * as Command from "@effect/cli/Command"
import { login } from "./login.ts"
import { logout } from "./logout.ts"
import { token } from "./token.ts"

export const auth = Command.make("auth").pipe(
  Command.withDescription("Commands to login and logout users from the cli"),
  Command.withSubcommands([login, logout, token]),
)
