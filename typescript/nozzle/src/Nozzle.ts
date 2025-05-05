import { Machine } from "@effect/experimental"
import type { CommandExecutor } from "@effect/platform"
import { Command as Cmd, FileSystem, Socket } from "@effect/platform"
import { Context, Data, Effect, Layer, Option, Request, Schedule, String } from "effect"
import * as Net from "node:net"
import * as EvmRpc from "./EvmRpc.js"
import * as ManifestDeployer from "./ManifestDeployer.js"
import type * as Model from "./Model.js"

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

    const create = fs.makeDirectory(directory, { recursive: true })
    const remove = fs.remove(directory, { recursive: true }).pipe(Effect.ignore)
    yield* remove
    yield* Effect.acquireRelease(create, () => remove).pipe(Effect.orDie)

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
      |provider = "anvil.toml"
    |`).trimStart()

    const provider = String.stripMargin(`|
      |url = "${rpc.url}"
    |`).trimStart()

    yield* fs.makeDirectory(`${directory}/data`).pipe(Effect.orDie)
    yield* fs.makeDirectory(`${directory}/datasets`).pipe(Effect.orDie)
    yield* fs.makeDirectory(`${directory}/providers`).pipe(Effect.orDie)
    yield* fs.writeFileString(`${directory}/config.toml`, config).pipe(Effect.orDie)
    yield* fs.writeFileString(`${directory}/providers/anvil.toml`, provider).pipe(Effect.orDie)
    yield* fs.writeFileString(`${directory}/datasets/anvil.toml`, dataset).pipe(Effect.orDie)

    const cmd = (cmd: string, ...args: Array<string>) =>
      Cmd.make(executable, cmd, ...args).pipe(Cmd.env({
        NOZZLE_CONFIG: `${directory}/config.toml`,
        NOZZLE_LOG: logging,
      }))

    const machine = Machine.make(
      Effect.gen(function*() {
        const state = {
          server: Option.none<CommandExecutor.Process>(),
          dataset: "anvil",
          block: BigInt(0),
        }

        return Machine.procedures.make(state).pipe(
          Machine.procedures.addPrivate<Exit>()("Exit", (ctx) =>
            // Check if the server was stopped intentionally or not.
            Option.match(ctx.state.server, {
              onNone: () => Effect.succeed([void 0, ctx.state]),
              onSome: () => Effect.die(new NozzleError({ message: "Server exited unexpectedly" })),
            })),
          Machine.procedures.add<Start>()("Start", (ctx) =>
            Effect.gen(function*() {
              if (Option.isSome(ctx.state.server)) {
                return [void 0, ctx.state] as const
              }

              // This effect starts the server in the background.
              const process = yield* cmd("server").pipe(
                Cmd.stdout("inherit"),
                Cmd.stderr("inherit"),
                Cmd.start,
                Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
              )

              // This effect waits for all ports to be open.
              // TODO: Remove the `sleep` and instead wait for a signal (e.g. from stdout) from the process.
              const healthy = Effect.sleep("200 millis").pipe(Effect.as(
                Effect.all([
                  waitForPort(1602),
                  waitForPort(1603),
                  waitForPort(1610),
                  waitForPort(1611),
                ], { concurrency: "unbounded" }).pipe(
                  Effect.timeout("10 seconds"),
                  Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
                ),
              ))

              // Whether the server exited with a non-zero exit code or not, at this point it's an error.
              const exit = process.exitCode.pipe(
                Effect.mapError((cause) => new NozzleError({ cause, message: "Server crashed" })),
                Effect.flatMap((code) =>
                  Effect.fail(new NozzleError({ message: `Server exited unexpectedly with code ${code}` }))
                ),
                Effect.asVoid,
              )

              // Wait for the server to either be up and running with all ports open or exit (crash).
              yield* Effect.raceFirst(healthy, exit)

              // Continue monitoring the server in the background.
              yield* ctx.fork(process.exitCode.pipe(Effect.ignore, Effect.zipRight(ctx.send(new Exit()))))

              return [void 0, { ...ctx.state, server: Option.some(process) }] as const
            })),
          Machine.procedures.add<Stop>()("Stop", (ctx) =>
            Effect.gen(function*() {
              if (Option.isNone(ctx.state.server)) {
                return [void 0, ctx.state] as const
              }

              yield* ctx.state.server.value.kill("SIGTERM").pipe(
                Effect.mapError((cause) => new NozzleError({ cause, message: "Failed to kill server" })),
              )

              return [void 0, { ...ctx.state, server: Option.none() }] as const
            })),
          Machine.procedures.add<Deploy>()("Deploy", (ctx) =>
            Effect.gen(function*() {
              yield* deployer.deploy(ctx.request.manifest).pipe(
                Effect.mapError((cause) => new NozzleError({ cause, message: "Failed to deploy manifest" })),
              )

              return [void 0, { ...ctx.state, dataset: ctx.request.manifest.name }] as const
            })),
          Machine.procedures.add<Dump>()("Dump", (ctx) =>
            Effect.gen(function*() {
              yield* cmd("dump", `--dataset=${ctx.state.dataset}`, `--end-block=${ctx.request.block}`).pipe(
                Cmd.stdout("inherit"),
                Cmd.stderr("inherit"),
                Cmd.exitCode,
                Effect.mapError((cause) => new NozzleError({ cause, message: "Deploy failed" })),
                Effect.filterOrFail((code) => code === 0, () => new NozzleError({ message: "Deploy failed" })),
              )

              return [void 0, { ...ctx.state, block: ctx.request.block }] as const
            })),
        )
      }),
    )

    const actor = yield* Machine.boot(machine)
    yield* actor.send(new Start()).pipe(
      // Ensure the server is stopped when the scope is closed.
      Effect.zip(Effect.addFinalizer(() => actor.send(new Stop()).pipe(Effect.ignore))),
    )

    return {
      join: actor.join.pipe(Effect.mapError((cause) => cause.cause as NozzleError)),
      start: actor.send(new Start()),
      stop: actor.send(new Stop()),
      dump: (block: bigint) => actor.send(new Dump({ block })),
      deploy: (manifest: Model.DatasetManifest) =>
        Effect.gen(function*() {
          const state = yield* actor.get
          if (state.dataset !== "anvil") {
            // TODO: Wiping the dataset and data directory here should not be necessary.
            yield* Effect.all([
              fs.remove(`${directory}/datasets/${state.dataset}.json`),
              fs.remove(`${directory}/data/${state.dataset}`, { recursive: true }),
            ]).pipe(Effect.ignore)
          }

          yield* actor.send(new Deploy({ manifest }))
        }),
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
}

class Exit extends Request.TaggedClass("Exit")<void, never, {}> {}
class Start extends Request.TaggedClass("Start")<void, NozzleError, {}> {}
class Stop extends Request.TaggedClass("Stop")<void, NozzleError, {}> {}
class Deploy extends Request.TaggedClass("Deploy")<void, NozzleError, DeployPayload> {}
class Dump extends Request.TaggedClass("Dump")<void, NozzleError, DumpPayload> {}
