import { Context, Data, Effect, Layer, Option, PubSub, RcRef, Schedule, Stream } from "effect"
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
      transport: Viem.http(url, { retryCount: 0 }),
      pollingInterval: 1_000,
    })

    const blocks = yield* RcRef.make({
      acquire: Stream.asyncPush<Viem.Block, EvmRpcError>((emit) =>
        Effect.acquireRelease(
          Effect.sync(() =>
            rpc.watchBlocks({
              onBlock: (block) => emit.single(block),
              onError: (cause) => emit.fail(new EvmRpcError({ cause })),
              emitMissed: true,
            })
          ),
          (unwatch) => Effect.sync(unwatch),
        )
      ).pipe(
        Stream.retry(
          Schedule.exponential("1 second").pipe(
            Schedule.jittered,
            Schedule.union(Schedule.spaced("10 seconds")),
            Schedule.upTo("3 seconds"),
            Schedule.tapInput(() => Effect.logWarning("Failed to connect to chain. Retrying ...")),
          ),
        ),
        Stream.changesWith((a, b) => a.hash === b.hash),
        Stream.mapAccumEffect(Option.none<Viem.Block>(), (previous, current) =>
          Effect.gen(function*() {
            if (Option.isSome(previous)) {
              if (previous.value.hash !== current.parentHash) {
                yield* Effect.fail(new EvmRpcError({ cause: "Chain reorg detected" }))
              }
            }

            return [Option.some(current), current]
          })),
        Stream.toPubSub({ capacity: "unbounded", replay: 1 }),
      ),
      idleTimeToLive: "10 seconds",
    })

    const watchChainHead = RcRef.get(blocks).pipe(
      Effect.flatMap(PubSub.subscribe),
      Effect.map(Stream.fromQueue),
      Effect.map(Stream.flattenTake),
      Stream.unwrapScoped,
      Stream.buffer({ capacity: 1, strategy: "sliding" }),
    )

    return { url, blocks: watchChainHead }
  })
