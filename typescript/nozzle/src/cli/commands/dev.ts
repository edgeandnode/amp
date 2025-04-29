import { Command, HelpDoc, Options, ValidationError } from "@effect/cli"
import { FileSystem } from "@effect/platform"
import { Config, Effect, Either, Fiber, Layer, Option, Schema, Stream } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"
import * as Model from "../../Model.js"
import * as Nozzle from "../../Nozzle.js"

export const dev = Command.make("dev", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription(
        "The dataset definition config file to build to a manifest",
      ),
      Options.mapEffect(Option.match({
        onSome: (file) => Effect.succeed(file),
        onNone: () =>
          ConfigLoader.ConfigLoader.pipe(
            Effect.flatMap((loader) => loader.find()),
            Effect.flatMap(Option.match({
              onSome: (file) => Effect.succeed(file),
              onNone: () => Effect.fail(ValidationError.invalidArgument(HelpDoc.p("No config file provided"))),
            })),
          ).pipe(Effect.provide(ConfigLoader.ConfigLoader.Default)),
      })),
    ),
    rpc: Options.text("rpc-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_RPC_URL").pipe(Config.withDefault("http://localhost:8545")),
      ),
      Options.withDescription("The url of the chain RPC server"),
    ),
    nozzle: Options.text("nozzle").pipe(
      Options.withDefault("nozzle"),
      Options.withAlias("n"),
      Options.withDescription(
        "The path of the nozzle executable",
      ),
    ),
  },
}).pipe(
  Command.withDescription("Run a dev server"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const configLoader = yield* ConfigLoader.ConfigLoader
      const manifestDeployer = yield* ManifestDeployer.ManifestDeployer
      const manifestBuilder = yield* ManifestBuilder.ManifestBuilder

      // TODO: Create a proper ConfigObserver service.
      const config = yield* fs.watch(args.config).pipe(
        Stream.filter((event) => (event._tag === "Update")),
        Stream.flatMap((_) => configLoader.load(args.config)),
        Stream.changesWith(Schema.equivalence(Model.DatasetDefinition)),
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.flatMap((definition) => manifestBuilder.build(definition)),
        Stream.changesWith(Schema.equivalence(Model.DatasetManifest)),
        Stream.flatMap((manifest) => manifestDeployer.deploy(manifest)),
        Stream.either,
        Stream.runForEach(Either.match({
          onLeft: (error) => Effect.logError(error),
          onRight: (message) => Effect.log(message),
        })),
        Effect.fork,
      )

      // TODO: Refactor all the workers into actors.
      const rpc = yield* EvmRpc.EvmRpc
      const nozzle = yield* Nozzle.Nozzle
      const server = yield* nozzle.start.pipe(Effect.flatMap(Effect.fork))
      const dump = yield* rpc.watchChainHead.pipe(
        Stream.changes,
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.runForEach((block) => nozzle.dump("anvil_rpc", block)),
        Effect.fork,
      )

      yield* Fiber.joinAll([config, server, dump])
    }).pipe(Effect.scoped)
  ),
  Command.provide(({ args }) =>
    Nozzle.Nozzle.withExecutable(args.nozzle).pipe(
      Layer.provideMerge(EvmRpc.EvmRpc.withUrl(args.rpc)),
    ).pipe(
      Layer.merge(ConfigLoader.ConfigLoader.Default),
      Layer.merge(ManifestBuilder.ManifestBuilder.Default.pipe(
        Layer.provide(Api.Registry.withUrl("http://localhost:1611")),
      )),
      Layer.merge(ManifestDeployer.ManifestDeployer.Default.pipe(
        Layer.provide(Api.Admin.withUrl("http://localhost:1610")),
      )),
    )
  ),
)
