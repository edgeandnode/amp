import * as Cli from "@effect/cli"
import { Command, FileSystem } from "@effect/platform"
import { Config, Console, Effect, Layer, Option, PubSub, Unify } from "effect"
import * as Viem from "viem"
import * as Chains from "viem/chains"
import * as Api from "../../Api.js"
import { ConfigLoader } from "../../ConfigLoader.js"
import { ManifestBuilder } from "../../ManifestBuilder.js"
import { ManifestDeployer } from "../../ManifestDeployer.js"
import { ManifestLoader } from "../../ManifestLoader.js"

export const dev = Cli.Command.make(
  "dev",
  {
    args: {
      config: Cli.Options.text("config").pipe(
        Cli.Options.optional,
        Cli.Options.withAlias("c"),
        Cli.Options.withDescription(
          "The dataset definition config file to build to a manifest",
        ),
      ),
      admin: Cli.Options.text("admin-url").pipe(
        Cli.Options.withFallbackConfig(
          Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
        ),
        Cli.Options.withDescription("The url of the Nozzle admin server"),
      ),
      registry: Cli.Options.text("registry-url").pipe(
        Cli.Options.withFallbackConfig(
          Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
        ),
        Cli.Options.withDescription("The url of the Nozzle registry server"),
      ),
      nozzle: Cli.Options.text("nozzle").pipe(
        Cli.Options.withDefault("nozzle"),
        Cli.Options.withAlias("n"),
        Cli.Options.withDescription(
          "The path of the nozzle executable",
        ),
      ),
      path: Cli.Options.text("path").pipe(
        Cli.Options.withDefault(".nozzle"),
        Cli.Options.withAlias("p"),
        Cli.Options.withDescription(
          "The path of the nozzle server configuration and data",
        ),
      ),
    },
  },
  ({ args }) =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      yield* initConfigDir(fs, args.path)
      yield* runServer(args.nozzle, args.path).pipe(
        Effect.tapError(Console.error),
        Effect.ignore,
        Effect.fork,
      )

      const configLoader = yield* ConfigLoader
      const manifestBuilder = yield* ManifestBuilder
      const manifestLoader = yield* ManifestLoader
      const manifestDeployer = yield* ManifestDeployer
      const manifest = yield* Unify.unify(
        Option.match(args.config, {
          onSome: (file) =>
            configLoader
              .load(file)
              .pipe(Effect.flatMap(manifestBuilder.build), Effect.map(Option.some)),
          onNone: () =>
            configLoader.find().pipe(
              Effect.flatMap(
                Unify.unify(
                  Option.match({
                    onSome: (definition) => manifestBuilder.build(definition).pipe(Effect.map(Option.some)),
                    onNone: () => manifestLoader.load("nozzle.json").pipe(Effect.map(Option.some)),
                  }),
                ),
              ),
            ),
        }),
      ).pipe(
        Effect.orDie,
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.dieMessage("No manifest or config file provided"),
            onSome: Effect.succeed,
          }),
        ),
      )

      const result = yield* manifestDeployer.deploy(manifest)
      yield* Effect.log(result)

      const rpc = Viem.createTestClient({
        mode: "anvil",
        chain: Chains.foundry,
        transport: Viem.http("http://localhost:8545"),
        pollingInterval: 1_000,
      }).extend(Viem.publicActions)
      const chainHead = yield* PubSub.sliding<bigint>(1)
      yield* watchChainHead(rpc, chainHead).pipe(
        Effect.tapError(Console.error),
        Effect.fork,
      )

      yield* Effect.gen(function*() {
        const blocks = yield* PubSub.subscribe(chainHead)
        while (true) {
          const block = yield* blocks.take
          yield* runDump(args.nozzle, args.path, manifest.name, block)
        }
      }).pipe(Effect.scoped)
    })
      .pipe(
        Effect.provide(
          layer
            .pipe(Layer.provide(Layer.mergeAll(
              Api.layerAdmin(args.admin),
              Api.layerRegistry(args.registry),
            ))),
        ),
      ),
).pipe(
  Cli.Command.withDescription("Run a dev server"),
)

const layer = Layer.mergeAll(
  ManifestBuilder.Default,
  ManifestLoader.Default,
  ManifestDeployer.Default,
  ConfigLoader.Default,
)

const initConfigDir = Effect.fn(function*(fs: FileSystem.FileSystem, path: string) {
  yield* fs.makeDirectory(path, { recursive: true })
  yield* fs.remove(`${path}/data`, { recursive: true }).pipe(Effect.ignore)
  yield* fs.makeDirectory(`${path}/data`, { recursive: true })
  yield* fs.makeDirectory(`${path}/datasets`, { recursive: true })
  yield* fs.makeDirectory(`${path}/providers`, { recursive: true })
  yield* fs.writeFileString(
    `${path}/config.toml`,
    [
      `data_dir = "data"`,
      `dataset_defs_dir = "datasets"`,
      `providers_dir = "providers"`,
      `max_mem_mb = 2000`,
      `spill_location = []`,
    ].join("\n"),
  )
  yield* fs.writeFileString(
    `${path}/providers/anvil_rpc.toml`,
    [`url = "http://localhost:8545"`].join("\n"),
  )
  yield* fs.writeFileString(
    `${path}/datasets/anvil_rpc.toml`,
    [
      `name = "anvil_rpc"`,
      `network = "anvil"`,
      `kind = "evm-rpc"`,
      `provider = "anvil_rpc.toml"`,
    ].join("\n"),
  )
})

const runServer = (nozzlePath: string, configRoot: string) =>
  Command.make(nozzlePath, "server").pipe(
    Command.env({ NOZZLE_CONFIG: `${configRoot}/config.toml` }),
    Command.stdout("inherit"),
    Command.stderr("inherit"),
    Command.exitCode,
    Effect.map((exitCode) => Effect.fail(new Error(`nozzle server exit (${exitCode})`))),
  )

const watchChainHead = (
  rpc: Viem.PublicClient,
  chainHead: PubSub.PubSub<bigint>,
) =>
  Effect.try({
    try: () => {
      let lastBlock = Option.none<bigint>()
      rpc.watchBlockNumber({
        onBlockNumber: (block) => {
          if (Option.some(block) === lastBlock) return
          lastBlock = Option.some(block)
          Effect.runSync(chainHead.publish(block))
        },
      })
    },
    catch: (err) => new Error(`Failed to watch chain head: ${err}`),
  })

const runDump = (
  nozzlePath: string,
  configRoot: string,
  dataset: string,
  endBlock: bigint,
) =>
  Command.make(
    nozzlePath,
    "dump",
    `--dataset=${dataset}`,
    `--end-block=${endBlock.toString()}`,
  ).pipe(
    Command.env({ NOZZLE_CONFIG: `${configRoot}/config.toml` }),
    Command.stdout("inherit"),
    Command.stderr("inherit"),
    Command.exitCode,
    Effect.andThen((exitCode) =>
      exitCode === 0
        ? Effect.void
        : Effect.fail(new Error(`nozzle dump exit (${exitCode})`))
    ),
  )
