import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as AmpRegistry from "../../AmpRegistry.ts"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { configFile, ExitCode } from "../common.ts"

// const CLUSTER_ADMIN_URL = new URL("https://gateway.amp.staging.edgeandnode.com")
const CLUSTER_ADMIN_URL = new URL("http://localhost:1610")

export const publish = Command.make("publish", {
  args: {
    tag: Options.text("tag").pipe(
      Options.withDescription("Dataset version (semver) tag"),
      Options.withSchema(Model.DatasetVersion),
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
  },
}).pipe(
  Command.withDescription("Publish a Dataset to the public registry"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const context = yield* ManifestContext.ManifestContext
      const auth = yield* Auth.AuthService
      const ampRegistry = yield* AmpRegistry.AmpRegistryService
      const client = yield* Admin.Admin

      const maybeAccessToken = yield* auth.get()
      if (Option.isNone(maybeAccessToken)) {
        yield* Console.error("Must be authenticated to publish your dataset. Run `amp auth login`")
        return yield* ExitCode.NonZero
      }
      const accessToken = maybeAccessToken.value

      const metadata = context.metadata

      yield* Console.info("Registering your Dataset with Amp")
      yield* client.registerDataset(metadata.namespace, metadata.name, context.manifest, args.tag).pipe(
        Effect.tap(() => Console.info("Dataset successfully registered with Amp")),
      )

      yield* Console.info(
        `Deploying your Dataset to Amp ${metadata.namespace}/${metadata.name}@${args.tag}. This will start indexing`,
      )
      yield* client.deployDataset(metadata.namespace, metadata.name, args.tag).pipe(
        Effect.tap(() => Console.info("Dataset successfully deployed to Amp")),
        Effect.catchTag("DatasetNotFound", (e) =>
          Console.error(
            `Failure deploying dataset ${metadata.namespace}/${metadata.name}@${args.tag}. Dataset not found`,
          ).pipe(Effect.zipRight(Effect.fail(e)))),
      )

      yield* Console.info("Publishing your Dataset to the registry")
      const publishResult = yield* ampRegistry.publishFlow({
        auth: accessToken,
        context,
        versionTag: args.tag,
        changelog: Option.getOrUndefined(args.changelog),
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
        `Visit https://playground.amp.edgeandnode.com/playground/${publishResult.namespace}/${publishResult.name}/${publishResult.revision} to view and query your Dataset`,
      )
      return yield* ExitCode.Zero
    })
  ),
  Command.provide(({ args }) =>
    Layer.mergeAll(
      Auth.layer,
      AmpRegistry.layer,
      ManifestContext.layerFromConfigFile(args.configFile),
    ).pipe(
      Layer.provideMerge(Admin.layer(`${CLUSTER_ADMIN_URL}`)),
    )
  ),
)
