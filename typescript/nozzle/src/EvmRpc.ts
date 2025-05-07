import { Context, Data, Effect, Layer, Option, Queue, RcRef, Schedule, Stream } from "effect"
import * as Viem from "viem"
import * as Chains from "viem/chains"

export class EvmRpc extends Context.Tag("Nozzle/EvmRpc")<EvmRpc, Effect.Effect.Success<ReturnType<typeof make>>>() {
  static withUrl(url: string) {
    return make(url).pipe(Layer.scoped(EvmRpc))
  }
}

export class EvmRpcError extends Data.TaggedError("EvmRpcError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

const make = (url: string) =>
  Effect.gen(function*() {
    const rpc = Viem.createPublicClient({
      chain: Chains.foundry,
      transport: Viem.http(url, { retryCount: 0 }),
    })

    const latest = Effect.tryPromise({
      try: () => rpc.getBlockNumber({ cacheTime: 0 }),
      catch: (cause) => new EvmRpcError({ message: "Failed to get block number", cause }),
    })

    const block = Effect.fnUntraced(function*(block: bigint) {
      return yield* Effect.tryPromise({
        try: () => rpc.getBlock({ blockNumber: block }),
        catch: (cause) => new EvmRpcError({ message: "Failed to get block", cause }),
      })
    })

    const stream = Stream.repeatEffectWithSchedule(latest, Schedule.fixed("1 second")).pipe(
      Stream.changes,
      Stream.mapAccumEffect(Option.none<bigint>(), (state, current) => {
        if (Option.isNone(state)) {
          return block(current).pipe(Effect.map((block) => [Option.some(current), Stream.succeed(block)]))
        }

        const range = Stream.range(Number(state.value) + 1, Number(current))
        const blocks = range.pipe(Stream.mapEffect((number) => block(BigInt(number))))
        return Effect.succeed([Option.some(current), blocks])
      }),
      Stream.flatMap((_) => _),
      Stream.changesWith((a, b) => a.hash === b.hash),
      Stream.retry(
        Schedule.exponential("1 second").pipe(
          Schedule.jittered,
          Schedule.union(Schedule.spaced("10 seconds")),
          Schedule.tapInput(() => Effect.logWarning("Failed to connect to chain. Retrying ...")),
        ),
      ),
    )

    const blocks = yield* RcRef.make({
      acquire: Stream.broadcastDynamic(stream, 4096),
      idleTimeToLive: "10 seconds",
    })

    return {
      url,
      blocks: RcRef.get(blocks).pipe(Effect.flatMap(Effect.fnUntraced(function*(stream) {
        const queue = yield* Queue.bounded<Stream.Stream.Success<typeof stream>>(4096)
        // TODO: Refactor this to use `Stream.fromQueue` / `Stream.toQueue` once `flattenTake` behavior is fixed.
        yield* stream.pipe(Stream.runForEach((block) => Queue.offer(queue, block)), Effect.forkScoped)
        return Stream.fromQueue(queue, { shutdown: true })
      }))).pipe(Stream.unwrapScoped),
    }
  })
