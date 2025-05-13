import { BigInt, Context, Data, Effect, Function, Layer, Mailbox, Option, Schedule, Stream } from "effect"
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

    const getLatestBlockNumber = Effect.tryPromise({
      try: () => rpc.getBlockNumber({ cacheTime: 0 }),
      catch: (cause) => new EvmRpcError({ message: "Failed to get latest block number", cause }),
    })

    const getBlockByNumber = Effect.fnUntraced(function*(number: bigint) {
      return yield* Effect.tryPromise({
        try: () => rpc.getBlock({ blockNumber: number, includeTransactions: false }),
        catch: (cause) => new EvmRpcError({ message: `Failed to fetch block number ${number}`, cause }),
      })
    })

    const sharedBlocks = yield* Stream.repeatEffectWithSchedule(getLatestBlockNumber, Schedule.fixed("1 second")).pipe(
      Stream.changes,
      Stream.mapAccumEffect(Option.none<bigint>(), (state, current) => {
        if (Option.isNone(state)) {
          return getBlockByNumber(current).pipe(Effect.map((block) => [Option.some(current), Stream.succeed(block)]))
        }

        const blocks = Stream.iterate(BigInt.increment(state.value), BigInt.increment).pipe(
          Stream.takeWhile(BigInt.lessThan(current)),
          Stream.mapEffect(getBlockByNumber),
        )

        return Effect.succeed([Option.some(current), blocks])
      }),
      Stream.flatMap(Function.identity),
      Stream.changesWith((a, b) => a.hash === b.hash),
      Stream.retry(
        Schedule.exponential("1 second").pipe(
          Schedule.jittered,
          Schedule.union(Schedule.spaced("10 seconds")),
          Schedule.tapInput(() => Effect.logWarning("Failed to connect to chain. Retrying ...")),
        ),
      ),
      Stream.orDie, // We've eliminated all potential errors from the stream due to the infinite retry..
      Stream.share({ capacity: 4096, strategy: "suspend" }),
    )

    return {
      url,
      blocks: Mailbox.fromStream(sharedBlocks, { capacity: 4096, strategy: "suspend" }).pipe(
        Effect.map(Mailbox.toStream),
        Stream.unwrapScoped,
      ),
    }
  })
