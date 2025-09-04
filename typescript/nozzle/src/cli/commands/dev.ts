import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export const dev = Command.make("dev", {
  args: {
    config: Options.file("config").pipe(
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to build to a manifest"),
      Options.optional,
    ),
    rpc: Options.text("rpc-url").pipe(
      Options.withFallbackConfig(Config.string("NOZZLE_RPC_URL").pipe(Config.withDefault("http://localhost:8545"))),
      Options.withDescription("The url of the chain RPC server"),
      Options.withSchema(Schema.URL),
    ),
    nozzle: Options.text("nozzle").pipe(
      Options.withAlias("n"),
      Options.withDescription("The path of the nozzle executable"),
      Options.withDefault("nozzle"),
    ),
  },
}).pipe(
  Command.withDescription("Run a dev server"),
  Command.withHandler(
    Effect.fn(function*() {
      return yield* Effect.dieMessage("Not implemented")
    }),
  ),
)
