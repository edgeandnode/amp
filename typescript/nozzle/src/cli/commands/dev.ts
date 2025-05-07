import { Command, Options } from "@effect/cli"
import { Path } from "@effect/platform"
import { Chunk, Config, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import type * as Viem from "viem"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"
import * as Nozzle from "../../Nozzle.js"
import type { NozzleError } from "../../Nozzle.js"
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
        Stream.runForEach((manifest) =>
          nozzle.deploy(manifest).pipe(Effect.catchAllCause(Utils.logCauseWith("Failed to deploy manifest")))
        ),
        Effect.orDie,
        Effect.forkScoped,
      )

      const rpc = yield* EvmRpc.EvmRpc
      const dump = yield* rpc.blocks.pipe(Stream.chunks).pipe(
        Stream.mapAccumEffect(Option.none<Viem.Hash>(), (state, chunk) => {
          const last = Chunk.last(chunk)
          if (Option.isNone(last)) {
            return Effect.succeed([state, Chunk.empty<Effect.Effect<void, NozzleError, never>>()])
          }

          if (Option.isNone(state)) {
            return Effect.logDebug(`Resetting data for block ${last.value.number} after restart`).pipe(
              Effect.as([Option.some(last.value.hash), Chunk.of(nozzle.dump(last.value.number, true))]),
            )
          }

          if (hasReorg(state.value, chunk)) {
            return Effect.logDebug(`Resetting data for block ${last.value.number} after block reorg`).pipe(
              Effect.as([Option.some(last.value.hash), Chunk.of(nozzle.dump(last.value.number, true))]),
            )
          }

          return Effect.logDebug(`Dumping data for block ${last.value.number}`).pipe(
            Effect.as([Option.some(last.value.hash), Chunk.of(nozzle.dump(last.value.number, false))]),
          )
        }),
        Stream.flattenChunks,
        Stream.runForEach((_) => _),
        Effect.orDie,
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
