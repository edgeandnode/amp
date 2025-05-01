import { Command, HelpDoc, Options, ValidationError } from "@effect/cli"
import { Config, Data, Effect, Either, Equal, Fiber, Layer, Option, Predicate, Schema, Stream } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"
import * as Model from "../../Model.js"
import * as Nozzle from "../../Nozzle.js"

export const dev = Command.make("dev", {
  args: {
    config: Options.file("config").pipe(
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
      Options.withSchema(Schema.URL),
    ),
    directory: Options.directory("directory", { exists: "no" }).pipe(
      Options.withDefault(".nozzle"),
      Options.withAlias("d"),
      Options.withDescription("The directory to run the dev server in"),
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
      const nozzle = yield* Nozzle.Nozzle
      const server = yield* nozzle.start.pipe(Effect.flatMap(Effect.fork))

      const config = yield* ConfigLoader.ConfigLoader
      const deployer = yield* ManifestDeployer.ManifestDeployer
      const builder = yield* ManifestBuilder.ManifestBuilder
      const manifest = config.watch(args.config).pipe(
        Stream.mapEffect((either) =>
          Effect.gen(function*() {
            if (Either.isLeft(either)) {
              return yield* Effect.fail(either.left)
            }

            return yield* builder.build(either.right)
          }).pipe(Effect.either)
        ),
        Stream.changesWith(Either.getEquivalence({
          right: Schema.equivalence(Model.DatasetManifest),
          left: Equal.equals,
        })),
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.map(Either.map((manifest) => new Manifest({ manifest }))),
        Stream.tap(Either.match({
          onLeft: (error) => Effect.logError(error),
          onRight: () => Effect.void,
        })),
        Stream.filterMap(Either.getRight),
      )

      const rpc = yield* EvmRpc.EvmRpc
      const blocks = rpc.blocks.pipe(
        Stream.map((block) => new Block({ block })),
      )

      const handle = Effect.fnUntraced(function*(dataset: string, value: Manifest | Block) {
        if (Manifest.is(value)) {
          yield* deployer.deploy(value.manifest)
        } else {
          yield* nozzle.dump(dataset, value.block)
        }

        return Manifest.is(value) ? value.manifest.name : dataset
      })

      const dump = yield* Stream.merge(blocks, manifest).pipe(
        Stream.runFoldEffect("anvil_rpc", handle),
        Effect.asVoid,
        Effect.fork,
      )

      yield* Fiber.joinAll([dump, server])
    }).pipe(Effect.scoped)
  ),
  Command.provide(({ args }) =>
    Nozzle.Nozzle.layer({
      executable: args.nozzle,
      directory: ".nozzle",
    }).pipe(
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
  readonly manifest: Model.DatasetManifest
}> {
  static is = Predicate.isTagged("Manifest") as (value: unknown) => value is Manifest
}

class Block extends Data.TaggedClass("Block")<{
  readonly block: bigint
}> {
  static is = Predicate.isTagged("Block") as (value: unknown) => value is Block
}
