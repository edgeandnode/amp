import { Context, Effect, Layer, Option, Stream } from "effect"
import * as Viem from "viem"
import * as Chains from "viem/chains"

export class EvmRpc extends Context.Tag("Nozzle/EvmRpc")<EvmRpc, ReturnType<typeof make>>() { }

const make = (url: string) => {
  const rpc = Viem.createPublicClient({
    chain: Chains.foundry,
    transport: Viem.http(url),
    pollingInterval: 1_000,
  })
  const watchChainHead = () =>
    Stream.asyncPush<bigint, Error>((emit) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          // watchBlockNumber initially returns 0 until the chain progresses,
          // so we avoid notifying consumers on repeated values.
          let prevBlock = Option.none<bigint>()
          return rpc.watchBlockNumber({
            onBlockNumber: (block) => {
              if (prevBlock === Option.some(block)) return
              prevBlock = Option.some(block)
              emit.single(block)
            },
            onError: (err) => console.error(err.message),
          })
        }),
        (unwatch) => Effect.sync(unwatch),
      )
    ).pipe(Stream.toPubSub({ capacity: 1, strategy: "sliding" }))

  return { watchChainHead }
}

export const layer = (url: string) => Layer.sync(EvmRpc, () => make(url))
