"use client"

import type { UseMutationOptions } from "@tanstack/react-query"
import { mutationOptions, useMutation } from "@tanstack/react-query"

import * as Constants from "../constants.js"

const datasetMutationOptions = mutationOptions<
  ReadonlyArray<any>,
  Error,
  Readonly<{ query: string }>
>({
  mutationKey: ["Dataset", "Query"] as const,
  async mutationFn(vars) {
    try {
      const response = await fetch(`${Constants.API_ORIGIN}/query`, {
        method: "POST",
        body: JSON.stringify(vars),
      })
      if (response.status !== 200) {
        throw new Error(
          `Query endpoint did not return 200 [${response.status}]`,
        )
      }
      const json = await response.json()

      return json as ReadonlyArray<any>
    } catch (err) {
      console.error("Failure querying dataset", { err })
      throw err
    }
  },
})

export function useDatasetsMutation(
  options: Omit<
    UseMutationOptions<ReadonlyArray<any>, Error, Readonly<{ query: string }>>,
    "mutationKey" | "mutationFn"
  > = {},
) {
  return useMutation({
    ...datasetMutationOptions,
    ...options,
  })
}
