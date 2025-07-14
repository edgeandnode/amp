"use client"

import type { UseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useQuery } from "@tanstack/react-query"
import { Array as EffectArray, Data, Schema, String as EffectString } from "effect"
import * as Model from "nozzl/Model"

import { API_ORIGIN } from "../constants.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const QueryEventStreamInstanceDecoder = Schema.decodeUnknownSync(Schema.parseJson(Model.QueryableEventStream))
const QueryableEventStreamTransformer = Schema.transform(
  // Response type comes as an encoded string prepended with "data: ". this denotes it as a SSE
  Schema.NonEmptyTrimmedString.pipe(Schema.filter((val) => EffectString.startsWith("data: ")(val))),
  // Decode it to a Model.QueryableEventStream instance,
  Model.QueryableEventStream,
  {
    strict: true,
    // Transformer to take the received encoded string source to the Model.QueryEventStream
    decode(input) {
      if (EffectString.isEmpty(input) || !EffectString.startsWith("data: ")(input)) {
        return Model.QueryableEventStream.make({ events:[] })
      }
      // parse string into Model.QueryableEventStream
      return QueryEventStreamInstanceDecoder(EffectString.slice(6)(input))
    },
    // Reverse to encode back to a string
    encode(queryableEventStream) {
      const jsonData = JSON.stringify(queryableEventStream)
      const sseData = `data: ${jsonData}\n\n`
      return encoder.encode(sseData).toString()
    },
  }
)
const QueryableEventStreamDecoder = Schema.decodeUnknownSync(QueryableEventStreamTransformer)

export const useQueryableEventsOptions = queryOptions({
  queryKey: ["QueryableEvents", "list"] as const,
  async queryFn(): Promise<ReadonlyArray<Model.QueryableEvent>> {
    const response = await fetch(`${API_ORIGIN}/events/stream`)

    if (!response.ok) {
      throw new FetchQueryableEventsError({message:`Failed to fetch events: ${response.statusText}`})
    }

    const reader = response.body?.getReader()
    if (!reader) {
      throw new FetchQueryableEventsError({message: "No response body available"})
    }

    let buffer = ""
    let events: ReadonlyArray<Model.QueryableEvent> = []

    try {
      let done = false
      while (!done) {
        const result = await reader.read()
        done = result.done

        if (!done && result.value) {
          buffer += decoder.decode(result.value, { stream: true })

          const lines = EffectString.split("\n")(buffer)
          buffer = lines.pop() || ""

          for (const line of lines) {
            try {
              const parsed = QueryableEventStreamDecoder(line)
              events = EffectArray.appendAll(events, parsed.events)
            } catch (error) {
              console.warn("Failed parsing QueryableEvent data from stream", {error})
            }
          }
        }
      }
    } finally {
      reader.releaseLock()
    }

    return events
  },
})

export function useQueryableEvents(
  options: Omit<
    UseQueryOptions<
      ReadonlyArray<Model.QueryableEvent>,
      Error,
      ReadonlyArray<Model.QueryableEvent>,
      readonly ["QueryableEvents", "list"]
    >,
    "queryKey" | "queryFn"
  > = {},
) {
  return useQuery({
    ...useQueryableEventsOptions,
    ...options,
  })
}

export class FetchQueryableEventsError extends Data.TaggedError("Nozzle/studio/errors/FetchQueryableEventsError")<{
  readonly message: string
}>{}
