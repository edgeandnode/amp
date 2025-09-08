import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Schema from "effect/Schema"

export const registryUrl = Options.text("registry-url").pipe(
  Options.withFallbackConfig(
    Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
  ),
  Options.withDescription("The url of the registry server"),
  Options.withSchema(Schema.URL),
)

export const adminUrl = Options.text("admin-url").pipe(
  Options.withFallbackConfig(
    Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
  ),
  Options.withDescription("The url of the admin server"),
  Options.withSchema(Schema.URL),
)

export const flightUrl = Options.text("flight-url").pipe(
  Options.withFallbackConfig(
    Config.string("NOZZLE_ARROW_FLIGHT_URL").pipe(Config.withDefault("http://localhost:1602")),
  ),
  Options.withDescription("The Arrow Flight URL to use for the proxy"),
  Options.withSchema(Schema.URL),
)

export const jsonLinesUrl = Options.text("json-lines-url").pipe(
  Options.withFallbackConfig(
    Config.string("NOZZLE_JSON_LINES_URL").pipe(Config.withDefault("http://localhost:1603")),
  ),
  Options.withDescription("The url of the json-lines server"),
  Options.withSchema(Schema.URL),
)

export const manifestFile = Options.file("manifest", { exists: "yes" }).pipe(
  Options.withAlias("m"),
  Options.withDescription("The dataset manifest file"),
)

export const configFile = Options.file("config", { exists: "yes" }).pipe(
  Options.withAlias("c"),
  Options.withDescription("The dataset definition config file"),
)

export const force = Options.boolean("force").pipe(
  Options.withAlias("f"),
  Options.withDefault(false),
  Options.withDescription("Skip confirmation prompts"),
)
