"use client"

import type { UseQueryOptions, UseSuspenseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useQuery, useSuspenseQuery } from "@tanstack/react-query"

import { USER_DEFINED_FUNCTIONS, type UserDefinedFunction } from "@/constants"

export const udfQueryOptions = queryOptions({
  queryKey: ["Schema", "UDF"] as const,
  async queryFn() {
    return await Promise.resolve(USER_DEFINED_FUNCTIONS)
  },
})

export function useUDFQuery(
  options: Omit<
    UseQueryOptions<
      ReadonlyArray<UserDefinedFunction>,
      Error,
      ReadonlyArray<UserDefinedFunction>,
      readonly ["Schema", "UDF"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useQuery({
    ...udfQueryOptions,
    ...options,
  })
}

export function useUDFSuspenseQuery(
  options: Omit<
    UseSuspenseQueryOptions<
      ReadonlyArray<UserDefinedFunction>,
      Error,
      ReadonlyArray<UserDefinedFunction>,
      readonly ["Schema", "UDF"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useSuspenseQuery({
    ...udfQueryOptions,
    staleTime: Number.POSITIVE_INFINITY,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    ...options,
  })
}
