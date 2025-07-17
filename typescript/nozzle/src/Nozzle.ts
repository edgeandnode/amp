import { Machine } from "@effect/experimental"
import { Command as Cmd, FileSystem, Socket } from "@effect/platform"
import { Context, Data, Effect, Fiber, Layer, Request, Schedule, String } from "effect"
import * as Net from "node:net"
import * as EvmRpc from "./EvmRpc.ts"
import * as ManifestDeployer from "./ManifestDeployer.ts"
import type * as Model from "./Model.ts"

export class NozzleError extends Data.TaggedError("NozzleError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export interface NozzleConfig {
  executable: string
  directory: string
  logging?: "error" | "warn" | "info" | "debug" | "trace"
}

export class Nozzle extends Context.Tag("Nozzle/Nozzle")<Nozzle, Effect.Effect.Success<ReturnType<typeof make>>>() {
  static layer(config: NozzleConfig) {
    return make(config).pipe(Layer.scoped(this))
  }
}

const make = ({
  directory,
  executable,
  logging = "info",
}: NozzleConfig) =>
  Effect.gen(function*() {
    const rpc = yield* EvmRpc.EvmRpc
    const fs = yield* FileSystem.FileSystem
    const deployer = yield* ManifestDeployer.ManifestDeployer

    yield* fs.makeDirectory(directory).pipe(
      Effect.zipRight(Effect.addFinalizer(() => fs.remove(directory, { recursive: true }).pipe(Effect.ignore))),
      Effect.orDie,
    )

    const config = String.stripMargin(`|
      |data_dir = "data"
      |dataset_defs_dir = "datasets"
      |providers_dir = "providers"
      |max_mem_mb = 2000
      |spill_location = []
    |`).trimStart()

    const dataset = String.stripMargin(`|
      |name = "anvil"
      |network = "anvil"
      |kind = "evm-rpc"
    |`).trimStart()

    const provider = String.stripMargin(`|
      |kind = "evm-rpc"
      |network = "anvil"
      |url = "${rpc.url}"
    |`).trimStart()

    yield* Effect.all([
      fs.makeDirectory(`${directory}/data`),
      fs.makeDirectory(`${directory}/datasets`),
      fs.makeDirectory(`${directory}/providers`),
    ]).pipe(Effect.orDie)

    yield* Effect.all([
      fs.writeFileString(`${directory}/config.toml`, config),
      fs.writeFileString(`${directory}/providers/anvil.toml`, provider),
      fs.writeFileString(`${directory}/datasets/anvil.toml`, dataset),
    ]).pipe(Effect.orDie)

    const cmd = (cmd: string, ...args: Array<string>) =>
      Cmd.make(executable, cmd, ...args).pipe(Cmd.env({
        NOZZLE_CONFIG: `${directory}/config.toml`,
        NOZZLE_LOG: logging,
      }))

    // This effect starts the server in the background.
    const process = yield* Effect.acquireRelease(
      Effect.gen(function*() {
        const process = yield* cmd("server", "--dev").pipe(
          Cmd.stdout("inherit"),
          Cmd.stderr("inherit"),
          Cmd.start,
          Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
        )

        // This effect waits for all ports to be open.
        const healthy = Effect.all([
          waitForPort(1602),
          waitForPort(1603),
          waitForPort(1610),
          waitForPort(1611),
        ], { concurrency: "unbounded" }).pipe(
          Effect.timeout("10 seconds"),
          Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
          Effect.interruptible,
        )

        // Whether the server exited with a non-zero exit code or not, at this point it's an error.
        const exit = process.exitCode.pipe(
          Effect.mapError((cause) => new NozzleError({ cause, message: "Server crashed" })),
          Effect.zipRight(Effect.fail(new NozzleError({ message: "Server exited unexpectedly" }))),
          Effect.interruptible,
        )

        // Wait for the server to either be up and running with all ports open or exit (crash).
        return yield* Effect.raceFirst(healthy, exit).pipe(Effect.as(process))
      }),
      (process) => process.kill("SIGTERM").pipe(Effect.ignore),
    )

    // Continue monitoring the server in the background.
    const server = yield* process.exitCode.pipe(
      Effect.mapError((cause) => new NozzleError({ cause, message: "Server crashed" })),
      Effect.zipRight(Effect.fail(new NozzleError({ message: "Server exited unexpectedly" }))),
      Effect.forkScoped,
    )

    const machine = Machine.make(
      Machine.procedures.make("anvil").pipe(
        Machine.procedures.add<Deploy>()("Deploy", (ctx) =>
          Effect.gen(function*() {
            yield* Effect.logDebug(`Deploying manifest "${ctx.request.manifest.name}"`)

            if (ctx.state !== "anvil") {
              // TODO: Resetting a specific dataset should be exposed via the control plane.
              yield* fs.remove(`${directory}/datasets/${ctx.state}.json`).pipe(Effect.ignore)
              yield* fs.remove(`${directory}/data/${ctx.state}`, { recursive: true }).pipe(Effect.ignore)
            }

            yield* deployer.deploy(ctx.request.manifest).pipe(
              Effect.mapError((cause) => new NozzleError({ cause, message: "Failed to deploy manifest" })),
            )

            return [void 0, ctx.request.manifest.name] as const
          })),
        Machine.procedures.add<Dump>()("Dump", (ctx) =>
          Effect.gen(function*() {
            yield* Effect.logDebug(`Dumping data for dataset "${ctx.state}" up to block ${ctx.request.block}`)

            // TODO: Resetting globally should be exposed via the control plane.
            if (ctx.request.reset) {
              yield* fs.remove(`${directory}/data`, { recursive: true }).pipe(Effect.ignore)
              yield* fs.makeDirectory(`${directory}/data`).pipe(Effect.ignore)
            }

            yield* cmd("dump", `--dataset=${ctx.state}`, `--end-block=${ctx.request.block}`).pipe(
              Cmd.stdout("inherit"),
              Cmd.stderr("inherit"),
              Cmd.exitCode,
              Effect.mapError((cause) => new NozzleError({ cause, message: "Deploy failed" })),
              Effect.filterOrFail((code) => code === 0, () => new NozzleError({ message: "Deploy failed" })),
            )

            return [void 0, ctx.state] as const
          })),
      ),
    )

    const actor = yield* Machine.boot(machine)
    const join = Effect.raceFirst(actor.join, Fiber.join(server))
    const dump = (block: bigint, reset = false) => actor.send(new Dump({ block, reset }))
    const deploy = (manifest: Model.DatasetManifest) => actor.send(new Deploy({ manifest }))

    return {
      join,
      dump,
      deploy,
    }
  })

const waitForPort = (port: number) =>
  Effect.async<Net.Socket, Socket.SocketError, never>((resume, signal) => {
    const connection = Net.createConnection({ port, signal })
    connection.on("connect", () => {
      connection.removeAllListeners()
      resume(Effect.succeed(connection))
    })
    connection.on("error", (cause) => {
      connection.removeAllListeners()
      resume(Effect.fail(new Socket.SocketGenericError({ reason: "Open", cause })))
    })
  }).pipe(
    Effect.retry({
      schedule: Schedule.spaced("100 millis"),
      while: (cause) => Socket.isSocketError(cause) && cause.reason === "Open",
    }),
    Effect.tap((connection) => Effect.try(() => connection.destroy()).pipe(Effect.ignore)),
    Effect.asVoid,
  )

interface DeployPayload {
  manifest: Model.DatasetManifest
}

interface DumpPayload {
  block: bigint
  reset: boolean
}

class Deploy extends Request.TaggedClass("Deploy")<void, NozzleError, DeployPayload> {}
class Dump extends Request.TaggedClass("Dump")<void, NozzleError, DumpPayload> {}
