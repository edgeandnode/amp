import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as AmpRegistry from "../../AmpRegistry.ts"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

export const publish = Command.make("publish", {
  args: {
    tag: Args.text({ name: "tag" }).pipe(
      Args.withDescription("Dataset version (semver) or 'dev' tag"),
      Args.withSchema(Schema.Union(Model.DatasetVersion, Model.DatasetDevTag)),
      Args.optional,
    ),
    changelog: Options.text("changelog").pipe(
      Options.withDescription(
        "Provide changelog details of what was changed or developed in this version. Helpful for users of your dataset to understand what changed with this version",
      ),
      Options.withFallbackPrompt(
        Prompt.text({ message: "Provide a changelog of what was introduced or changed with this dataset version" }),
      ),
      Options.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Publish a Dataset to the public registry"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const context = yield* ManifestContext.ManifestContext
      const auth = yield* Auth.AuthService
      const ampRegistry = yield* AmpRegistry.AmpRegistryService

      const maybeAccessToken = yield* auth.get()
      if (Option.isNone(maybeAccessToken)) {
        return yield* Console.error("Must be authenticated to publish your dataset. Run `amp auth login`")
      }
      const accessToken = maybeAccessToken.value

      // Determine version tag (handle optional, default to "dev")
      const versionTag = Option.getOrElse(args.tag, () => "dev" as const)
      const status = versionTag === "dev" ? "draft" : "published"

      const publishResult = yield* ampRegistry.publishFlow({
        auth: accessToken,
        context,
        versionTag,
        changelog: Option.getOrUndefined(args.changelog),
        status,
      }).pipe(
        Effect.tap((result) => Console.log(`Published ${result.namespace}/${result.name}@${result.revision}`)),
        Effect.catchTags({
          "Amp/Registry/Errors/DatasetOwnershipError": (err) =>
            Console.error(`Cannot publish to ${err.namespace}/${err.name}`).pipe(
              Effect.zipRight(Console.error("Dataset already exists.")),
              Effect.zipRight(Effect.fail(err)),
            ),
          "Amp/Registry/Errors/VersionAlreadyExistsError": (err) =>
            Console.error(`Version ${err.versionTag} already exists for ${err.namespace}/${err.name}`).pipe(
              Effect.zipRight(Console.error(`Choose a different version tag or update the existing version`)),
              Effect.zipRight(Effect.fail(err)),
            ),
          "Amp/Registry/Errors/RegistryApiError": (err) =>
            Console.error(`Registry API error (${err.status}): ${err.errorCode}`).pipe(
              Effect.zipRight(Console.error(`${err.message}`)),
              Effect.zipRight(err.requestId ? Console.error(`Request ID: ${err.requestId}`) : Effect.void),
              Effect.zipRight(Effect.fail(err)),
            ),
        }),
      )

      yield* Console.log("Dataset successfully published!")
      yield* Console.log(
        `Visit https://registry.amp.edgeandnode.com/playground/${publishResult.namespace}/${publishResult.name}/${publishResult.revision} to view and query your Dataset`,
      )
    })
  ),
  Command.provide(Auth.layer),
  Command.provide(AmpRegistry.layer),
  Command.provide(({ args }) =>
    ManifestContext.layerFromConfigFile(args.configFile)
      .pipe(
        Layer.provideMerge(Admin.layer(`${args.adminUrl}`)),
      )
  ),
)
