import { queryOptions, useQuery, type UseQueryOptions } from "@tanstack/react-query"

import { runQuery } from "./nozzle.client"

export const buildERC20QueryOptions = queryOptions({
  queryKey: ["transfers", "mainner", "erc20", { limit: 100 }] as const,
  async queryFn() {
    return await runQuery("SELECT * FROM transfers_eth_mainnet.erc20_transfers LIMIT 100")
  }
})

export function useERC20Transfers(
  options: Omit<
    UseQueryOptions<
      ReadonlyArray<any> | undefined,
      Error,
      ReadonlyArray<any> | undefined,
      readonly ["transfers", "mainner", "erc20", {
        readonly limit: 100
      }]
    >,
    "queryKey" | "queryFn"
  > = {}
) {
  return useQuery({
    ...buildERC20QueryOptions,
    ...options
  })
}
