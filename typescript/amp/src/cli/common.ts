import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Schema from "effect/Schema"

export const adminUrl = Options.text("admin-url").pipe(
  Options.withFallbackConfig(
    Config.string("AMP_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
  ),
  Options.withDescription("The url of the admin server"),
  Options.withSchema(Schema.URL),
)

export const flightUrl = Options.text("flight-url").pipe(
  Options.withFallbackConfig(
    Config.string("AMP_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
  ),
  Options.withDescription("The Arrow Flight URL to use for the proxy"),
  Options.withSchema(Schema.URL),
)

export const configFile = Options.file("config", { exists: "yes" }).pipe(
  Options.withAlias("c"),
  Options.withDescription("The dataset definition config file"),
)
