"use client"

import { Schema } from "effect"
import type { QueryableEvent } from "nozzl/Studio/Model"
import { QueryableEventStream } from "nozzl/Studio/Model"
import { useCallback, useEffect, useReducer, useRef } from "react"

import * as Constants from "../constants.js"

const QueryEventStreamInstanceDecoder = Schema.decodeUnknownSync(
  Schema.parseJson(QueryableEventStream),
)

export interface UseQueryableEventsQueryOptions {
  enabled?: boolean
  onSuccess?: (data: ReadonlyArray<QueryableEvent>) => void
  onError?: (error: Error) => void
  retry?: boolean
  retryDelay?: number
}

export interface State {
  data: ReadonlyArray<QueryableEvent>
  error?: Error | null | undefined
  status: "idle" | "fetching" | "success" | "error"
}
export type Action =
  | { type: "FETCHING" }
  | { type: "SUCCESS"; payload: ReadonlyArray<QueryableEvent> }
  | { type: "ERROR"; payload: Error }
  | { type: "RESET" }

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "FETCHING": {
      return {
        ...state,
        status: "fetching",
        error: null,
      }
    }
    case "ERROR": {
      return {
        ...state,
        error: action.payload,
        status: "error",
      }
    }
    case "SUCCESS": {
      return {
        ...state,
        data: action.payload,
        status: "success",
      }
    }
    case "RESET": {
      return {
        ...state,
        data: [],
        error: undefined,
        status: "idle",
      }
    }
    default: {
      return state
    }
  }
}

export function useQueryableEventsQuery({
  enabled = true,
  onError,
  onSuccess,
  retry = true,
  retryDelay = 1000,
}: Readonly<UseQueryableEventsQueryOptions> = {}) {
  const [state, dispatch] = useReducer(reducer, {
    data: [],
    error: undefined,
    status: "idle",
  })

  // Stable refs
  const eventSourceRef = useRef<EventSource | null>(null)
  const retryTimeoutRef = useRef<NodeJS.Timeout | null>(null)
  const mountedRef = useRef(true)

  // Callback refs
  const callbacksRef = useRef({
    onSuccess,
    onError,
    retry,
    retryDelay,
    enabled,
  })

  // Update callback refs without causing effects to re-run
  useEffect(() => {
    callbacksRef.current = {
      onSuccess,
      onError,
      retry,
      retryDelay,
      enabled,
    }
  })

  const cleanup = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
      eventSourceRef.current = null
    }
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current)
    }
  }, [])

  const connect = useCallback(() => {
    cleanup()

    if (!callbacksRef.current.enabled) return

    if (!mountedRef.current) return

    dispatch({ type: "FETCHING" })

    try {
      const es = new EventSource(`${Constants.API_ORIGIN}/events/stream`)

      const handleMessage = (event: MessageEvent) => {
        if (!mountedRef.current) return

        try {
          const parsedData = QueryEventStreamInstanceDecoder(event.data)

          dispatch({ type: "SUCCESS", payload: parsedData.events })
          callbacksRef.current.onSuccess?.(parsedData.events)
        } catch (e) {
          const error =
            e instanceof Error ? e : new Error("Failed to parse SSE data")
          dispatch({ type: "ERROR", payload: error })
          callbacksRef.current.onError?.(error)
        }
      }

      const handleError = () => {
        if (!mountedRef.current) return

        const error = new Error("SSE connection error")
        dispatch({ type: "ERROR", payload: error })
        callbacksRef.current.onError?.(error)

        cleanup()

        if (callbacksRef.current.retry && callbacksRef.current.enabled) {
          retryTimeoutRef.current = setTimeout(
            connect,
            callbacksRef.current.retryDelay,
          )
        }
      }

      es.addEventListener("message", handleMessage)
      es.addEventListener("error", handleError)

      eventSourceRef.current = es
    } catch (e) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (!mountedRef.current) return

      const error =
        e instanceof Error ? e : new Error("Failed to create SSE connection")
      dispatch({ type: "ERROR", payload: error })
      callbacksRef.current.onError?.(error)
    }
  }, [cleanup])

  // Handle component lifecycle
  useEffect(() => {
    mountedRef.current = true
    connect()

    return () => {
      mountedRef.current = false
      cleanup()
    }
  }, [connect, cleanup])

  // Handle enabled changes
  useEffect(() => {
    if (enabled) {
      connect()
    } else {
      cleanup()
    }
  }, [enabled, connect, cleanup])

  return {
    data: state.data,
    error: state.error,
    isLoading: state.status === "fetching",
    isError: state.status === "error",
    isSuccess: state.status === "success",
    refetch: connect,
  }
}
