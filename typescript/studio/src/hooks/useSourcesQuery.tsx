"use client"

import { StudioModel } from "@edgeandnode/amp"
import type { UseSuspenseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useSuspenseQuery } from "@tanstack/react-query"
import { Schema } from "effect"

import * as Constants from "../constants.js"

export const sourcesQueryOptions = queryOptions({
  queryKey: ["Query", "Sources"] as const,
  async queryFn() {
    const response = await fetch(`${Constants.API_ORIGIN}/sources`, {
      method: "GET",
    })
    if (response.status !== 200) {
      throw new Error(`Sources endpoint did not return 200 [${response.status}]`)
    }
    const json = await response.json()

    return Schema.decodeUnknownSync(Schema.Array(StudioModel.DatasetSource))(json)
  },
})

export function useSourcesSuspenseQuery(
  options: Omit<
    UseSuspenseQueryOptions<
      ReadonlyArray<StudioModel.DatasetSource>,
      Error,
      ReadonlyArray<StudioModel.DatasetSource>,
      readonly ["Query", "Sources"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useSuspenseQuery({
    ...sourcesQueryOptions,
    ...options,
  })
}
