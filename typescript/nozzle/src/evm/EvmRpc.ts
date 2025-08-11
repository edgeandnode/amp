import * as BigInt from "effect/BigInt"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Function from "effect/Function"
import * as Layer from "effect/Layer"
import * as Mailbox from "effect/Mailbox"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import * as Viem from "viem"
import * as Chains from "viem/chains"

/**
 * Service definition for the evm rpc service.
 */
export class EvmRpc extends Context.Tag("Nozzle/EvmRpc")<EvmRpc, {
  /**
   * The URL of the RPC server.
   */
  readonly url: string
  /**
   * Get a block by number.
   *
   * @param number - The number of the block to get.
   * @returns The block.
   */
  readonly getBlockByNumber: (number: bigint) => Effect.Effect<Viem.Block<bigint, false, "latest">, EvmRpcError>
  /**
   * Get the latest block number.
   */
  readonly getLatestBlockNumber: Effect.Effect<bigint, EvmRpcError>
  /**
   * Stream blocks monotonically increasing in block number.
   *
   * In case of a reorg, the stream will be reset to the latest block number.
   */
  readonly streamBlocks: Stream.Stream<Viem.Block<bigint, false, "latest">, EvmRpcError>
}>() {}

export class EvmRpcError extends Data.TaggedError("EvmRpcError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

const make = (url: string) =>
  Effect.gen(function*() {
    const rpc = Viem.createPublicClient({ chain: Chains.foundry, transport: Viem.http(url, { retryCount: 0 }) })

    const getLatestBlockNumber = Effect.tryPromise({
      try: () => rpc.getBlockNumber({ cacheTime: 0 }),
      catch: (cause) => new EvmRpcError({ message: "Failed to get latest block number", cause }),
    })

    const getBlockByNumber = Effect.fn(function*(number: bigint) {
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
      getBlockByNumber,
      getLatestBlockNumber,
      streamBlocks: Mailbox.fromStream(sharedBlocks, { capacity: 4096, strategy: "suspend" }).pipe(
        Effect.map(Mailbox.toStream),
        Stream.unwrapScoped,
      ),
    }
  })

/**
 * Creates a layer for the evm rpc service.
 *
 * @param url - The url of the rpc server.
 * @returns A layer for the evm rpc service.
 */
export const layer = (url: string) => make(url).pipe(Layer.scoped(EvmRpc))
