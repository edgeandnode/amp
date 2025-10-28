import * as Options from "@effect/cli/Options"
import * as Config from "effect/Config"
import * as Schema from "effect/Schema"
import * as Model from "../Model.ts"

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

/**
 * Dataset reference option in format: namespace/name@version
 *
 * Examples: my_namespace/my_dataset@1.0.0, edgeandnode/eth_mainnet@1.0.0
 */
export const datasetReference = Options.text("reference").pipe(
  Options.withDescription("Dataset reference in format: namespace/name@version"),
  Options.withSchema(Model.Reference),
)

/**
 * Parse a dataset reference string into DatasetMetadata
 *
 * @param ref - Reference string in format "namespace/name@version"
 * @returns DatasetMetadata with namespace, name, and version
 */
export const parseReferenceToMetadata = (ref: string): Model.DatasetMetadata => {
  const { name, namespace, version } = Model.parseReference(ref)
  return new Model.DatasetMetadata({ namespace, name, version })
}
