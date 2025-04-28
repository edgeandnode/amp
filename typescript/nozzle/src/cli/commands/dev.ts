import { Command, Options } from "@effect/cli"
import { Command as Cmd, FileSystem } from "@effect/platform"
import { Config, Effect, Fiber, Layer, Option, Stream } from "effect"
import * as Api from "../../Api.js"
import * as ConfigLoader from "../../ConfigLoader.js"
import * as EvmRpc from "../../EvmRpc.js"
import * as ManifestBuilder from "../../ManifestBuilder.js"
import * as ManifestDeployer from "../../ManifestDeployer.js"

export const dev = Command.make("dev", {
  args: {
    config: Options.text("config").pipe(
      Options.optional,
      Options.withAlias("c"),
      Options.withDescription(
        "The dataset definition config file to build to a manifest",
      ),
    ),
    admin: Options.text("admin-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_ADMIN_URL").pipe(Config.withDefault("http://localhost:1610")),
      ),
      Options.withDescription("The url of the Nozzle admin server"),
    ),
    registry: Options.text("registry-url").pipe(
      Options.withFallbackConfig(
        Config.string("NOZZLE_REGISTRY_URL").pipe(Config.withDefault("http://localhost:1611")),
      ),
      Options.withDescription("The url of the Nozzle registry server"),
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
    path: Options.text("path").pipe(
      Options.withDefault(".nozzle"),
      Options.withAlias("p"),
      Options.withDescription(
        "The path of the nozzle server configuration and data",
      ),
    ),
  },
}).pipe(
  Command.withDescription("Run a dev server"),
  Command.withHandler(({ args }) =>
    Effect.gen(function*() {
      const configLoader = yield* ConfigLoader.ConfigLoader
      const configPath = yield* args.config.pipe(
        Option.map(Effect.succeed),
        Option.getOrElse(() =>
          configLoader.find().pipe(Effect.map(Option.getOrThrowWith(() =>
            new ConfigLoader.ConfigLoaderError({ message: "Failed to load dataset definition file" })
          )))
        ),
      )

      yield* initConfigDir(args.path)
      const server = yield* runServer(args.nozzle, args.path).pipe(Effect.fork)

      const manifestDeployer = yield* ManifestDeployer.ManifestDeployer
      const rpc = yield* EvmRpc.EvmRpc
      const dataset = yield* Effect.gen(function*() {
        yield* Effect.sleep(200)
        const manifest = yield* buildManifest(configPath)
        const result = yield* manifestDeployer.deploy(manifest)
        yield* Effect.log(result)

        yield* rpc.watchChainHead.pipe(
          Stream.runForEach((block) =>
            runDump(args.nozzle, args.path, manifest.name, block)
          ),
        )
      }).pipe(Effect.fork)

      yield* Fiber.joinAll([server, dataset])
    }).pipe(Effect.scoped)
  ),
  Command.provide(({ args }) =>
    Layer.mergeAll(
      ConfigLoader.ConfigLoader.Default,
      ManifestBuilder.ManifestBuilder.Default,
      ManifestDeployer.ManifestDeployer.Default,
      EvmRpc.EvmRpc.withUrl(args.rpc),
    ).pipe(Layer.provide(Layer.mergeAll(
      Api.Admin.withUrl(args.admin),
      Api.Registry.withUrl(args.registry),
    )))
  ),
)

const initConfigDir = Effect.fn(function*(path: string) {
  const fs = yield* FileSystem.FileSystem
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
  Cmd.make(nozzlePath, "server").pipe(
    Cmd.env({ NOZZLE_CONFIG: `${configRoot}/config.toml` }),
    Cmd.stdout("inherit"),
    Cmd.stderr("inherit"),
    Cmd.exitCode,
    Effect.flatMap((exitCode) => Effect.fail(new Error(`nozzle server exit (${exitCode})`))),
  )

const buildManifest = Effect.fn(function*(configPath: string) {
  const configLoader = yield* ConfigLoader.ConfigLoader
  const manifestBuilder = yield* ManifestBuilder.ManifestBuilder
  const definition = yield* configLoader.load(configPath)
  return yield* manifestBuilder.build(definition)
})

const runDump = (
  nozzlePath: string,
  configRoot: string,
  dataset: string,
  endBlock: bigint,
) =>
  Cmd.make(
    nozzlePath,
    "dump",
    `--dataset=${dataset}`,
    `--end-block=${endBlock.toString()}`,
  ).pipe(
    Cmd.env({ NOZZLE_CONFIG: `${configRoot}/config.toml` }),
    Cmd.stdout("inherit"),
    Cmd.stderr("inherit"),
    Cmd.exitCode,
    Effect.andThen((exitCode) =>
      exitCode === 0
        ? Effect.void
        : Effect.fail(new Error(`nozzle dump exit (${exitCode})`))
    ),
  )
