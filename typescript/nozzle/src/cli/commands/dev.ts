import { Command, Options } from "@effect/cli"
import { Path } from "@effect/platform"
import { Config, Data, Effect, Fiber, Layer, Option, Predicate, Schema, Stream } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"
import type * as Model from "../../Model.js"
import * as Nozzle from "../../Nozzle.js"
import * as Utils from "../../Utils.js"

export const dev = Command.make("dev", {
  args: {
    config: Options.file("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription("The dataset definition config file to build to a manifest"),
    ),
    rpc: Options.text("rpc-url").pipe(
      Options.withFallbackConfig(Config.string("NOZZLE_RPC_URL").pipe(Config.withDefault("http://localhost:8545"))),
      Options.withDescription("The url of the chain RPC server"),
      Options.withSchema(Schema.URL),
    ),
    directory: Options.directory("directory", { exists: "no" }).pipe(
      Options.withDefault(".nozzle"),
      Options.withAlias("d"),
      Options.withDescription("The directory to run the dev server in"),
      Options.mapEffect((dir) => Effect.map(Path.Path, (path) => path.resolve(dir))),
    ),
    nozzle: Options.text("nozzle").pipe(
      Options.withDefault("nozzle"),
      Options.withAlias("n"),
      Options.withDescription("The path of the nozzle executable"),
    ),
  },
}).pipe(
  Command.withDescription("Run a dev server"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const config = yield* ConfigLoader.ConfigLoader
      const file = yield* Option.match(args.config, {
        onSome: (file) => Effect.succeed(file),
        onNone: () =>
          config.find().pipe(
            Effect.flatMap(Option.match({
              onSome: (file) => Effect.succeed(file),
              onNone: () => Effect.dieMessage("No config file provided"),
            })),
          ),
      })

      const manifest = config.watch(file, {
        onError: (cause) => Effect.logError("Failed to load config", Utils.prettyCause(cause)),
      }).pipe(Stream.map((manifest) => new Manifest({ manifest })))

      const rpc = yield* EvmRpc.EvmRpc
      const blocks = rpc.blocks.pipe(
        Stream.filter((block) => block.number !== null),
        Stream.map((block) => new Block({ block: block.number! })),
      )

      // TODO: Move this all to the nozzle actor.
      const nozzle = yield* Nozzle.Nozzle
      const dump = yield* Stream.merge(blocks, manifest).pipe(
        Stream.runForEach(Effect.fnUntraced(function*(message) {
          if (Manifest.is(message)) {
            yield* nozzle.deploy(message.manifest).pipe(
              Effect.catchAllCause((cause) => Effect.logError("Failed to deploy manifest", Utils.prettyCause(cause))),
            )
          } else {
            yield* nozzle.dump(message.block).pipe(
              Effect.catchAllCause((cause) => Effect.logError("Failed to dump block", Utils.prettyCause(cause))),
            )
          }
        })),
        Effect.fork,
      )

      yield* Effect.raceFirst(Fiber.join(dump), nozzle.join)
    })
  ),
  Command.provide(({ args }) =>
    Nozzle.Nozzle.layer({
      executable: args.nozzle,
      directory: args.directory,
    }).pipe(
      Layer.provideMerge(EvmRpc.EvmRpc.withUrl(`${args.rpc}`)),
      Layer.provide(ManifestDeployer.ManifestDeployer.Default.pipe(
        Layer.provide(Api.Admin.withUrl("http://localhost:1610")),
      )),
    ).pipe(
      Layer.merge(ConfigLoader.ConfigLoader.Default.pipe(
        Layer.provide(Api.Registry.withUrl("http://localhost:1611")),
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
