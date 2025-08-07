"use client"

import type { UseSuspenseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useSuspenseQuery } from "@tanstack/react-query"
import { Schema } from "effect"
import { DatasetMetadata } from "nozzl/Studio/Model"

import * as Constants from "../constants.js"

export const metadataQueryOptions = queryOptions({
  queryKey: ["Query", "Metadata"] as const,
  async queryFn() {
    const response = await fetch(`${Constants.API_ORIGIN}/metadata`, {
      method: "GET",
    })
    if (response.status !== 200) {
      throw new Error(
        `Metadata endpoint did not return 200 [${response.status}]`,
      )
    }
    const json = await response.json()

    return Schema.decodeUnknownSync(DatasetMetadata)(json)
  },
})

export function useMetadataSuspenseQuery(
  options: Omit<
    UseSuspenseQueryOptions<
      DatasetMetadata,
      Error,
      DatasetMetadata,
      readonly ["Query", "Metadata"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useSuspenseQuery({
    ...metadataQueryOptions,
    ...options,
  })
}
