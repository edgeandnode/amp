import * as Cli from "@effect/cli"
import { Command, FileSystem } from "@effect/platform"
import { Config, Effect, Layer, Option, Stream, Unify } from "effect"
import * as Api from "../../Api.js"
import { ConfigLoader } from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
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
      rpc: Cli.Options.text("rpc-url").pipe(
        Cli.Options.withFallbackConfig(
          Config.string("NOZZLE_RPC_URL").pipe(Config.withDefault("http://localhost:8545")),
        ),
        Cli.Options.withDescription("The url of the chain RPC server"),
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
      yield* runServer(args.nozzle, args.path).pipe(Effect.fork)

      const manifestDeployer = yield* ManifestDeployer
      const manifest = yield* loadManifest(args.config)
      const result = yield* manifestDeployer.deploy(manifest)
      yield* Effect.log(result)

      const rpc = yield* EvmRpc.EvmRpc
      const chainHead = yield* rpc.watchChainHead()
      yield* Stream.fromPubSub(chainHead).pipe(
        Stream.flattenTake,
        Stream.runForEach((block) => runDump(args.nozzle, args.path, manifest.name, block)),
      )
    })
      .pipe(
        Effect.scoped,
        Effect.provide(layer(args.admin, args.registry, args.rpc)),
      ),
).pipe(
  Cli.Command.withDescription("Run a dev server"),
)

const layer = (admin: string, registry: string, rpc: string) =>
  Layer.mergeAll(
    ManifestBuilder.Default,
    ManifestLoader.Default,
    ManifestDeployer.Default,
    ConfigLoader.Default,
    EvmRpc.layer(rpc),
  ).pipe(Layer.provide(Layer.mergeAll(
    Api.layerAdmin(admin),
    Api.layerRegistry(registry),
  )))

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
    Effect.flatMap((exitCode) => Effect.fail(new Error(`nozzle server exit (${exitCode})`))),
  )

const loadManifest = Effect.fn(function*(config: Option.Option<string>) {
  const configLoader = yield* ConfigLoader
  const manifestBuilder = yield* ManifestBuilder
  const manifestLoader = yield* ManifestLoader
  return yield* Unify.unify(
    Option.match(config, {
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
