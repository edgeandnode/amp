"use client"

import type { UseSuspenseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useSuspenseQuery } from "@tanstack/react-query"
import { Schema } from "effect"
import { DefaultQuery } from "nozzl/Studio/Model"

import * as Constants from "../constants.js"

export const defaultQueryOptions = queryOptions({
  queryKey: ["Query", "Default"] as const,
  async queryFn() {
    const response = await fetch(`${Constants.API_ORIGIN}/query/default`, {
      method: "GET",
    })
    if (response.status !== 200) {
      throw new Error(
        `Default query endpoint did not return 200 [${response.status}]`,
      )
    }
    const json = await response.json()

    return Schema.decodeUnknownSync(DefaultQuery)(json)
  },
})

export function useDefaultQuery(
  options: Omit<
    UseSuspenseQueryOptions<
      DefaultQuery,
      Error,
      DefaultQuery,
      readonly ["Query", "Default"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useSuspenseQuery({
    ...defaultQueryOptions,
    ...options,
  })
}
