import { Machine } from "@effect/experimental"
import { Command as Cmd, CommandExecutor, FileSystem, Socket } from "@effect/platform"
import { Context, Data, Effect, Layer, Option, Request, Schedule, Scope, String } from "effect"
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

    yield* Effect.acquireRelease(
      fs.makeDirectory(directory, { recursive: true }),
      () => fs.remove(directory, { recursive: true }).pipe(Effect.ignore),
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
      |provider = "anvil.toml"
    |`).trimStart()

    const provider = String.stripMargin(`|
      |url = "${rpc.url}"
    |`).trimStart()

    yield* fs.makeDirectory(`${directory}/data`)
    yield* fs.makeDirectory(`${directory}/datasets`)
    yield* fs.makeDirectory(`${directory}/providers`)
    yield* fs.writeFileString(`${directory}/config.toml`, config)
    yield* fs.writeFileString(`${directory}/providers/anvil.toml`, provider)
    yield* fs.writeFileString(`${directory}/datasets/anvil.toml`, dataset)

    const cmd = (cmd: string, ...args: Array<string>) =>
      Cmd.make(executable, cmd, ...args).pipe(Cmd.env({
        NOZZLE_CONFIG: `${directory}/config.toml`,
        NOZZLE_LOG: logging,
      }))

    const machine = Machine.make(
      Effect.gen(function*() {
        const scope = yield* Effect.scope
        const state = {
          server: Option.none<CommandExecutor.Process>(),
          dataset: "anvil",
          block: BigInt(0),
        }

        return Machine.procedures.make(state).pipe(
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
                  Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
                ),
              ))

              // This effect waits for the server to exit.
              const exit = process.exitCode.pipe(
                Effect.mapError((cause) => new NozzleError({ cause, message: "Server crashed" })),
                // Whether the server exited with a non-zero exit code or not, if the process ever ends, it's an error.
                Effect.zip(Effect.fail(new NozzleError({ message: "Server exited unexpectedly" }))),
                Effect.asVoid,
              )

              // Wait for the server to either be up and running with all ports open or exit (crash).
              yield* Effect.raceFirst(healthy, exit)

              // Once the server is healthy, we fork the running process and monitor it for an unexpected exit.
              yield* Effect.fork(exit)

              return [void 0, { ...ctx.state, server: Option.some(process) }] as const
            }).pipe(Scope.extend(scope), Effect.tapErrorCause((cause) => Effect.log("Error", cause)))),
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
              yield* cmd(
                "dump",
                `--dataset=${ctx.state.dataset}`,
                `--end-block=${ctx.request.block}`,
              ).pipe(
                Cmd.stdout("inherit"),
                Cmd.stderr("inherit"),
                Cmd.exitCode,
                Effect.mapError((cause) => new NozzleError({ cause, message: "Deploy failed" })),
                Effect.filterOrFail((code) => code === 0, () => new NozzleError({ message: "Deploy failed" })),
              )

              return [void 0, { ...ctx.state, block: ctx.request.block }] as const
            })),
        )
      }).pipe(
        // TODO: There's a bug in the type system here. As a workaround, we are referencing all required tags here.
        Effect.zipLeft(Effect.all([
          CommandExecutor.CommandExecutor,
        ])),
      ),
    )

    const actor = yield* Machine.boot(machine)
    yield* actor.send(new Start())

    return {
      join: actor.join,
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
  }).pipe(
    Effect.mapError((cause) => new NozzleError({ cause, message: "Failed to start Nozzle" })),
  )

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
    return Effect.sync(() => connection.destroy()).pipe(Effect.asVoid)
  }).pipe(
    Effect.retry({
      schedule: Schedule.spaced("1 second"),
      while: (cause) => Socket.isSocketError(cause) && cause.reason === "Open",
    }),
    Effect.tap((connection) => connection.destroy()),
  )

interface DeployPayload {
  manifest: Model.DatasetManifest
}

interface DumpPayload {
  block: bigint
}

class Start extends Request.TaggedClass("Start")<void, NozzleError, {}> {}
class Stop extends Request.TaggedClass("Stop")<void, NozzleError, {}> {}
class Deploy extends Request.TaggedClass("Deploy")<void, NozzleError, DeployPayload> {}
class Dump extends Request.TaggedClass("Dump")<void, NozzleError, DumpPayload> {}
