import { Command, Options } from "@effect/cli"
import { Path } from "@effect/platform"
import { Chunk, Config, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import type * as Viem from "viem"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
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

      const nozzle = yield* Nozzle.Nozzle
      const deploy = yield* config.watch(file, {
        onError: Utils.logCauseWith("Failed to load config"),
      }).pipe(
        Stream.buffer({ capacity: 1, strategy: "sliding" }),
        Stream.tap((manifest) =>
          nozzle.deploy(manifest).pipe(Effect.catchAllCause(Utils.logCauseWith("Failed to deploy manifest")))
        ),
        Stream.orDie,
        Stream.runDrain,
        Effect.forkScoped,
      )

      const rpc = yield* EvmRpc.EvmRpc
      const dump = yield* rpc.blocks.pipe(
        Stream.chunks,
        Stream.filter(Chunk.isNonEmpty),
        Stream.mapAccumEffect(Option.none<Viem.Hash>(), (state, chunk) => {
          const last = Chunk.lastNonEmpty(chunk)

          // Reset nozzle if there's no previous block hash or if a reorg is detected.
          return Option.isNone(state) || hasReorg(state.value, chunk) ?
            nozzle.reset(last.number).pipe(
              Effect.catchAllCause(Utils.logCauseWith("Failed to reset nozzle")),
              Effect.as([Option.some(last.hash), void 0] as const),
            ) :
            nozzle.dump(last.number).pipe(
              Effect.catchAllCause(Utils.logCauseWith("Failed to dump block")),
              Effect.as([Option.some(last.hash), void 0] as const),
            )
        }),
        Stream.orDie,
        Stream.runDrain,
        Effect.forkScoped,
      )

      yield* Effect.raceFirst(Fiber.joinAll([dump, deploy]), nozzle.join)
    }).pipe(Effect.scoped)
  ),
  Command.provide(({ args }) =>
    Nozzle.Nozzle.layer({
      executable: args.nozzle,
      directory: args.directory,
    }).pipe(
      Layer.provideMerge(EvmRpc.EvmRpc.withUrl(`${args.rpc}`)),
      Layer.provideMerge(Api.Admin.withUrl("http://localhost:1610")),
      Layer.merge(ConfigLoader.ConfigLoader.Default.pipe(
        Layer.provide(Api.Registry.withUrl("http://localhost:1611")),
      )),
    )
  ),
)

const hasReorg = (parent: Viem.Hash, chunk: Chunk.Chunk<Viem.Block<bigint, false, "latest">>) => {
  let previous = parent
  for (const block of chunk) {
    if (block.parentHash !== previous) {
      return true
    }
    previous = block.hash
  }

  return false
}
