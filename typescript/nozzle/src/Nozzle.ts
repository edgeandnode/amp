import { Command as Cmd, FileSystem, Socket } from "@effect/platform"
import { Context, Data, Effect, Layer, Schedule, String } from "effect"
import * as Net from "node:net"
import * as EvmRpc from "./EvmRpc.js"

export class NozzleError extends Data.TaggedError("NozzleError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export class Nozzle extends Context.Tag("Nozzle/Nozzle")<Nozzle, Effect.Effect.Success<ReturnType<typeof make>>>() {
  static withExecutable(executable: string) {
    return make(executable).pipe(Layer.scoped(this))
  }
}

const make = (executable: string) =>
  Effect.gen(function*() {
    const rpc = yield* EvmRpc.EvmRpc
    const fs = yield* FileSystem.FileSystem
    const dir = yield* fs.makeTempDirectoryScoped()

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

    yield* fs.makeDirectory(`${dir}/data`)
    yield* fs.makeDirectory(`${dir}/datasets`)
    yield* fs.makeDirectory(`${dir}/providers`)
    yield* fs.writeFileString(`${dir}/config.toml`, config)
    yield* fs.writeFileString(`${dir}/providers/anvil_rpc.toml`, provider)
    yield* fs.writeFileString(`${dir}/datasets/anvil_rpc.toml`, dataset)

    const run = (cmd: string, ...args: Array<string>) =>
      Cmd.make(executable, cmd, ...args).pipe(Cmd.env({ NOZZLE_CONFIG: `${dir}/config.toml` }))

    const start = run("server").pipe(
      Cmd.stdout("inherit"),
      Cmd.stderr("inherit"),
      Cmd.start,
      Effect.mapError((cause) => new NozzleError({ cause, message: "Server failed to start" })),
      // TODO: Remove this and instead wait for a signal (e.g. from stdout) from the process.
      Effect.zipLeft(Effect.sleep("1 second")),
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
            Effect.mapError((cause) => new NozzleError({ cause, message: `Server crashed` })),
            Effect.filterOrFail(
              (code) => code === 0,
              (code) => new NozzleError({ message: `Server crashed with exit code ${code}` }),
            ),
            Effect.asVoid,
          )

          return yield* ready.pipe(Effect.as(exit))
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
