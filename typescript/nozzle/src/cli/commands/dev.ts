import { Command, HelpDoc, Options, ValidationError } from "@effect/cli"
import { FileSystem } from "@effect/platform"
import { Config, Data, Effect, Either, Fiber, Layer, Option, Predicate, Ref, Schema, Stream, Unify } from "effect"
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
      // TODO: Refactor all the workers into actors.
      const nozzle = yield* Nozzle.Nozzle
      const server = yield* nozzle.start.pipe(Effect.flatMap(Effect.fork))

      const fs = yield* FileSystem.FileSystem
      const configLoader = yield* ConfigLoader.ConfigLoader
      const manifestBuilder = yield* ManifestBuilder.ManifestBuilder

      const load = configLoader.load(args.config)
      const updates = fs.watch(args.config).pipe(
        Stream.filter(Predicate.isTagged("Update")),
        Stream.flatMap(() => load),
      )

      const ref = yield* Ref.make("anvil_rpc")
      const manifest = Stream.concat(Stream.fromEffect(load), updates).pipe(
        Stream.changesWith(Schema.equivalence(Model.DatasetDefinition)),
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.flatMap((definition) => manifestBuilder.build(definition)),
        Stream.changesWith(Schema.equivalence(Model.DatasetManifest)),
        Stream.map((manifest) => new Manifest({ manifest })),
      )

      const rpc = yield* EvmRpc.EvmRpc
      const blocks = rpc.watchChainHead.pipe(
        Stream.changes,
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.map((block) => new Block({ block })),
      )

      const manifestDeployer = yield* ManifestDeployer.ManifestDeployer
      const dataset = yield* Stream.merge(manifest, blocks).pipe(
        Stream.either,
        Stream.runForEach((msg) =>
          Either.match(msg, {
            onLeft: (error) => Effect.logError(error),
            onRight: Unify.unify((msg) => {
              if (Block.is(msg)) {
                return Ref.get(ref).pipe(
                  Effect.flatMap((dataset) => nozzle.dump(dataset, msg.block)),
                  Effect.asVoid,
                )
              }

              if (Manifest.is(msg)) {
                return Ref.set(ref, msg.manifest.name).pipe(
                  Effect.zipRight(manifestDeployer.deploy(msg.manifest)),
                  Effect.asVoid,
                )
              }

              return Effect.void
            }),
          })
        ),
        Effect.fork,
      )

      yield* Fiber.joinAll([server, dataset])
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

class Manifest extends Data.TaggedClass("Manifest")<{
  manifest: Model.DatasetManifest
}> {
  static is = Predicate.isTagged("Manifest") as Predicate.Refinement<unknown, Manifest>
}

class Block extends Data.TaggedClass("Block")<{
  block: bigint
}> {
  static is = Predicate.isTagged("Block") as Predicate.Refinement<unknown, Block>
}
