import { Context, Data, Effect, Layer, PubSub, RcRef, Schedule, Stream } from "effect"
import * as Viem from "viem"
import * as Chains from "viem/chains"

export class EvmRpc extends Context.Tag("Nozzle/EvmRpc")<EvmRpc, Effect.Effect.Success<ReturnType<typeof make>>>() {
  static withUrl(url: string) {
    return make(url).pipe(Layer.scoped(EvmRpc))
  }
}

export class EvmRpcError extends Data.TaggedError("EvmRpcError")<{
  readonly cause: unknown
}> {}

const make = (url: string) =>
  Effect.gen(function*() {
    const rpc = Viem.createPublicClient({
      chain: Chains.foundry,
      transport: Viem.http(url),
      pollingInterval: 1_000,
    })

    const blocks = yield* RcRef.make({
      acquire: Stream.asyncPush<bigint, EvmRpcError>((emit) =>
        Effect.acquireRelease(
          Effect.sync(() =>
            rpc.watchBlockNumber({
              onBlockNumber: (block) => emit.single(block),
              onError: (error) => emit.fail(new EvmRpcError({ cause: error })),
              emitMissed: false,
              emitOnBegin: true,
            })
          ),
          (unwatch) => Effect.sync(unwatch),
        )
      ).pipe(
        Stream.changes,
        Stream.retry(
          Schedule.exponential("1 second").pipe(
            Schedule.jittered,
            Schedule.union(Schedule.spaced("10 seconds")),
            Schedule.upTo("1 minute"),
            Schedule.tapInput(() => Effect.log("Retrying...")),
          ),
        ),
        Stream.toPubSub({ capacity: 1, strategy: "sliding" }),
      ),
      idleTimeToLive: "10 seconds",
    })

    const watchChainHead = RcRef.get(blocks).pipe(
      Effect.flatMap(PubSub.subscribe),
      Effect.map(Stream.fromQueue),
      Effect.map(Stream.flattenTake),
      Stream.unwrapScoped,
    )

    return { url, blocks: watchChainHead }
  })
