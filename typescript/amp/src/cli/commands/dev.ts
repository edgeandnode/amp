import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Layer from "effect/Layer"
import * as Admin from "../../api/Admin.ts"
import * as ConfigLoader from "../../ConfigLoader.ts"
import * as DevServer from "../../DevServer.ts"

export const dev = Command.make("dev", {
  args: {
    adminUrl: Options.text("admin-url").pipe(
      Options.withDescription("The url of the admin api to use"),
      Options.withDefault("http://localhost:1610"),
    ),
  },
}).pipe(
  Command.withDescription("Run a development server with hot reloading"),
  Command.withHandler(() => DevServer.layer().pipe(Layer.launch)),
  Command.provide(({ args }) =>
    ConfigLoader.ConfigLoader.Default.pipe(Layer.provideMerge(Admin.layer(`${args.adminUrl}`)))
  ),
)
