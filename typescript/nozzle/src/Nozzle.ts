import { Command as Cmd, FileSystem, Path, Socket } from "@effect/platform"
import { Context, Data, Effect, FiberRef, Layer, Logger, Schedule, String } from "effect"
import * as Net from "node:net"
import * as EvmRpc from "./EvmRpc.js"

export class NozzleError extends Data.TaggedError("NozzleError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export interface NozzleConfig {
  executable: string
  directory: string
}

export class Nozzle extends Context.Tag("Nozzle/Nozzle")<Nozzle, Effect.Effect.Success<ReturnType<typeof make>>>() {
  static layer(config: NozzleConfig) {
    return make(config).pipe(Layer.scoped(this))
  }
}

const make = ({
  directory,
  executable,
}: NozzleConfig) =>
  Effect.gen(function*() {
    const rpc = yield* EvmRpc.EvmRpc
    const fs = yield* FileSystem.FileSystem

    yield* Effect.acquireRelease(
      fs.makeDirectory(directory, { recursive: true }),
      () => fs.remove(directory, { recursive: true }).pipe(Effect.ignore),
    )

    const config = String.stripMargin(`
      |data_dir = "data"
      |dataset_defs_dir = "datasets"
      |providers_dir = "providers"
      |max_mem_mb = 2000
      |spill_location = []
    `)

    const dataset = String.stripMargin(`
      |name = "anvil_rpc"
      |network = "anvil"
      |kind = "evm-rpc"
      |provider = "anvil_rpc.toml"
    `)

    const provider = String.stripMargin(`
      |url = "${rpc.url}"
    `)

    yield* fs.makeDirectory(`${directory}/data`)
    yield* fs.makeDirectory(`${directory}/datasets`)
    yield* fs.makeDirectory(`${directory}/providers`)
    yield* fs.writeFileString(`${directory}/toml`, config)
    yield* fs.writeFileString(`${directory}/providers/anvil_rpc.toml`, provider)
    yield* fs.writeFileString(`${directory}/datasets/anvil_rpc.toml`, dataset)

    yield* FiberRef.currentMinimumLogLevel
    const run = (cmd: string, ...args: Array<string>) =>
      Cmd.make(executable, cmd, ...args).pipe(Cmd.env({
        NOZZLE_CONFIG: `${directory}/toml`,
        NOZZLE_LOG: "info",
      }))

    const start = run("server").pipe(
      Cmd.stdout("inherit"),
      Cmd.stderr("inherit"),
      Cmd.start,
      Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
      // TODO: Remove this and instead wait for a signal (e.g. from stdout) from the process.
      Effect.zipLeft(Effect.sleep("200 millis")),
      Effect.flatMap((process) =>
        Effect.gen(function*() {
          const ready = Effect.all([
            wait(1602),
            wait(1603),
            wait(1610),
            wait(1611),
          ], { concurrency: "unbounded" }).pipe(
            Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
          )

          const exit = process.exitCode.pipe(
            Effect.mapError((cause) => new NozzleError({ cause, message: "Server crashed" })),
            // Whether the server exited with a non-zero exit code or not, if the process ever ends, it's an error.
            Effect.zip(Effect.fail(new NozzleError({ message: "Server exited unexpectedly" }))),
            Effect.asVoid,
          )

          return yield* ready.pipe(Effect.raceFirst(exit), Effect.as(exit))
        })
      ),
    )

    const dump = (dataset: string, block: bigint) =>
      run("dump", `--dataset=${dataset}`, `--end-block=${block}`).pipe(
        Cmd.stdout("inherit"),
        Cmd.stderr("inherit"),
        Cmd.exitCode,
        Effect.mapError((cause) => new NozzleError({ cause, message: "Dump failed" })),
        Effect.filterOrFail((code) => code === 0, () => new NozzleError({ message: "Dump failed" })),
        Effect.asVoid,
      )

    return { start, dump }
  }).pipe(
    Effect.mapError((cause) => new NozzleError({ cause, message: "Failed to start Nozzle" })),
  )

const wait = (port: number) =>
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
