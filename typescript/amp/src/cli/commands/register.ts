import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as String from "effect/String"
import * as AmpRegistry from "../../AmpRegistry.ts"
import * as Admin from "../../api/Admin.ts"
import * as Auth from "../../Auth.ts"
import * as ManifestContext from "../../ManifestContext.ts"
import * as Model from "../../Model.ts"
import { adminUrl, configFile } from "../common.ts"

export const register = Command.make("register", {
  args: {
    tag: Args.text({ name: "tag" }).pipe(
      Args.withDescription("Dataset version (semver) or 'dev' tag"),
      Args.withSchema(Schema.Union(Model.DatasetVersion, Model.DatasetDevTag)),
      Args.optional,
    ),
    publish: Options.boolean("publish", { ifPresent: true }).pipe(
      Options.withAlias("p"),
      Options.withDescription(
        "If present, will also publish the Dataset to the public registry (must be authenticated)",
      ),
      Options.optional,
    ),
    configFile: configFile.pipe(Options.optional),
    adminUrl,
  },
}).pipe(
  Command.withDescription("Register a dataset definition from config"),
  Command.withHandler(
    Effect.fn(function*({ args }) {
      const context = yield* ManifestContext.ManifestContext
      const client = yield* Admin.Admin
      const auth = yield* Auth.AuthService
      const ampRegistry = yield* AmpRegistry.AmpRegistryService

      // If tag is not provided or is "dev", register without version tag (bumps dev tag)
      // Otherwise, register with the specified semantic version
      const version = Option.match(args.tag, {
        onNone: () => Option.none(),
        onSome: (tag) => (Schema.is(Model.DatasetDevTag)(tag) ? Option.none() : Option.some(tag)),
      })

      const result = yield* client.registerDataset(
        context.metadata.namespace,
        context.metadata.name,
        version,
        context.manifest,
      )

      const publish = args.publish.pipe(Option.getOrElse(() => false))
      if (!publish) {
        return yield* Console.log(result)
      }

      const maybeAccessToken = yield* auth.get()
      if (Option.isNone(maybeAccessToken)) {
        yield* Console.warn(`To publish your Dataset, you must be authenticated. Run "amp auth login"`)
        yield* Console.log("Your Dataset was successfully registered", result)
        return
      }

      yield* Console.log("Dataset successfully registered. Working to publish now")

      const accessToken = maybeAccessToken.value

      const versionTag = Option.getOrElse(args.tag, () => "dev" as const)
      const status = versionTag === "dev" ? "draft" : "published"
      const changelog = yield* Prompt.text({
        message: "Provide a changelog of what was introduced or changed with this dataset version",
      })

      const publishResult = yield* ampRegistry.publishFlow({
        auth: accessToken,
        context,
        versionTag,
        changelog: String.isEmpty(changelog) ? undefined : changelog,
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
    }),
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
