"use client"

import { ManifestBuildResult } from "@edgeandnode/amp/ManifestBuilder"
import { Schema } from "effect"
import { useCallback, useEffect, useRef, useSyncExternalStore } from "react"

import * as Constants from "../constants.js"

const DatasetManifestInstanceDecoder = Schema.decodeUnknownSync(Schema.parseJson(ManifestBuildResult))
const datasetManifestEquivalence = Schema.equivalence(ManifestBuildResult)

export interface UseAmpConfigStreamQueryOptions {
  enabled?: boolean
  onSuccess?: (data: ManifestBuildResult) => void
  onError?: (error: Error) => void
  retry?: boolean
  retryDelay?: number
}

export interface State {
  data: ManifestBuildResult | null
  error?: Error | null | undefined
  status: "idle" | "fetching" | "success" | "error"
}

/**
 * Singleton SSE Connection Manager
 *
 * Manages a single EventSource connection shared across all hook instances.
 * This prevents multiple SSE connections from being created when the hook
 * is used in multiple components.
 *
 * @internal Exported for testing purposes only
 */
export class AmpConfigStreamManager {
  private static instance: AmpConfigStreamManager | null = null
  private eventSource: EventSource | null = null
  private retryTimeout: NodeJS.Timeout | null = null
  private subscribers = new Set<() => void>()
  private currentData: ManifestBuildResult | null = null
  private currentError: Error | null = null
  private status: "idle" | "connecting" | "connected" | "error" = "idle"
  private retryConfig = {
    enabled: true,
    delay: 1000,
  }
  private cachedSnapshot: State | null = null

  private constructor() {}

  static getInstance(): AmpConfigStreamManager {
    if (!AmpConfigStreamManager.instance) {
      AmpConfigStreamManager.instance = new AmpConfigStreamManager()
    }
    return AmpConfigStreamManager.instance
  }

  /**
   * Subscribe to state changes (for useSyncExternalStore)
   */
  subscribe(onStoreChange: () => void): () => void {
    this.subscribers.add(onStoreChange)

    // Start connection if not already connected
    // Set status to connecting synchronously before actually connecting
    // This ensures useSyncExternalStore sees the connecting state immediately
    if (!this.eventSource && this.status === "idle") {
      this.status = "connecting"
      this.connect()
    }

    // Return unsubscribe function
    return () => {
      this.subscribers.delete(onStoreChange)

      // Close connection if no more subscribers
      if (this.subscribers.size === 0) {
        this.disconnect()
      }
    }
  }

  /**
   * Get current state snapshot (for useSyncExternalStore)
   * Returns current state as new object only when state actually changed
   */
  getSnapshot(): State {
    // When idle with no data/error, treat as connecting to show immediate loading state
    // This handles the case where useSyncExternalStore calls getSnapshot before subscribe
    const effectiveStatus = this.status === "idle" && !this.currentData && !this.currentError
      ? "connecting"
      : this.status

    const mappedStatus: State["status"] = effectiveStatus === "connecting"
      ? "fetching"
      : effectiveStatus === "connected"
      ? "success"
      : effectiveStatus

    const newSnapshot: State = {
      data: this.currentData,
      error: this.currentError,
      status: mappedStatus,
    }

    // Only update cache if state actually changed
    if (!this.cachedSnapshot || !this.isSnapshotEqual(this.cachedSnapshot, newSnapshot)) {
      this.cachedSnapshot = newSnapshot
    }

    return this.cachedSnapshot
  }

  /**
   * Check if two snapshots are equal using Effect Schema equivalence for data
   */
  private isSnapshotEqual(a: State, b: State): boolean {
    // Status and error can use reference/primitive equality
    if (a.status !== b.status || a.error !== b.error) {
      return false
    }

    // Use Effect Schema equivalence for DatasetManifest comparison
    if (a.data === null && b.data === null) {
      return true
    }
    if (a.data === null || b.data === null) {
      return false
    }

    return datasetManifestEquivalence(a.data, b.data)
  }

  /**
   * Get current data (for callbacks)
   */
  getCurrentData(): ManifestBuildResult | null {
    return this.currentData
  }

  /**
   * Configure retry behavior
   */
  setRetryConfig(enabled: boolean, delay: number): void {
    this.retryConfig = { enabled, delay }
  }

  /**
   * Connect to SSE endpoint
   */
  private connect(): void {
    this.disconnect() // Clean up any existing connection

    this.status = "connecting"
    this.currentError = null
    this.notifySubscribers()

    try {
      const es = new EventSource(`${Constants.API_ORIGIN}/config/stream`)

      const handleMessage = (event: MessageEvent) => {
        try {
          const parsedData = DatasetManifestInstanceDecoder(event.data)

          this.currentData = parsedData
          this.currentError = null
          this.status = "connected"

          // Notify all subscribers of state change
          this.notifySubscribers()
        } catch (e) {
          const error = e instanceof Error ? e : new Error("Failed to parse SSE data")
          this.handleError(error)
        }
      }

      const handleError = () => {
        const error = new Error("SSE connection error")
        this.handleError(error)
      }

      es.addEventListener("message", handleMessage)
      es.addEventListener("error", handleError)

      this.eventSource = es
    } catch (e) {
      const error = e instanceof Error ? e : new Error("Failed to create SSE connection")
      this.handleError(error)
    }
  }

  /**
   * Handle connection errors
   */
  private handleError(error: Error): void {
    this.currentError = error
    this.status = "error"

    this.notifySubscribers()
    this.disconnect()

    // Retry if enabled and there are still subscribers
    if (this.retryConfig.enabled && this.subscribers.size > 0) {
      this.retryTimeout = setTimeout(() => {
        this.connect()
      }, this.retryConfig.delay)
    }
  }

  /**
   * Notify all subscribers of state change
   */
  private notifySubscribers(): void {
    for (const subscriber of this.subscribers) {
      subscriber()
    }
  }

  /**
   * Disconnect from SSE endpoint
   */
  private disconnect(): void {
    if (this.eventSource) {
      this.eventSource.close()
      this.eventSource = null
    }
    if (this.retryTimeout) {
      clearTimeout(this.retryTimeout)
      this.retryTimeout = null
    }
  }

  /**
   * Force reconnect
   */
  refetch(): void {
    this.connect()
  }

  /**
   * Clean up for testing
   */
  static reset(): void {
    if (AmpConfigStreamManager.instance) {
      AmpConfigStreamManager.instance.disconnect()
      AmpConfigStreamManager.instance = null
    }
  }
}

/**
 * Hook to stream amp config manifest updates via SSE
 *
 * Uses a singleton SSE connection shared across all instances of the hook.
 * This prevents multiple connections from being created when used in multiple components.
 *
 * @param options - Configuration options
 * @returns Query state with data, error, loading status, and refetch function
 */
export function useAmpConfigStreamQuery({
  enabled = true,
  onError,
  onSuccess,
  retry = true,
  retryDelay = 1000,
}: Readonly<UseAmpConfigStreamQueryOptions> = {}) {
  const manager = AmpConfigStreamManager.getInstance()

  // Configure retry settings
  useEffect(() => {
    manager.setRetryConfig(retry, retryDelay)
  }, [retry, retryDelay, manager])

  // Stable callback refs to avoid re-subscribing
  const onSuccessRef = useRef(onSuccess)
  const onErrorRef = useRef(onError)
  const prevDataRef = useRef<ManifestBuildResult | null>(null)
  const prevErrorRef = useRef<Error | null>(null)

  useEffect(() => {
    onSuccessRef.current = onSuccess
    onErrorRef.current = onError
  })

  // Subscribe to the external store using useSyncExternalStore
  const state = useSyncExternalStore(
    useCallback(
      (onStoreChange) => {
        if (!enabled) {
          return () => {}
        }
        return manager.subscribe(onStoreChange)
      },
      [enabled, manager],
    ),
    useCallback(() => manager.getSnapshot(), [manager]),
    useCallback(() => manager.getSnapshot(), [manager]), // Use same snapshot for SSR
  )

  // Call callbacks when data/error changes
  useEffect(() => {
    if (state.data !== prevDataRef.current && state.data !== null) {
      prevDataRef.current = state.data
      onSuccessRef.current?.(state.data)
    }
  }, [state.data])

  useEffect(() => {
    if (state.error !== prevErrorRef.current && state.error !== null && state.error !== undefined) {
      prevErrorRef.current = state.error
      onErrorRef.current?.(state.error)
    }
  }, [state.error])

  // Refetch function
  const refetch = useCallback(() => {
    manager.refetch()
  }, [manager])

  return {
    data: enabled ? state.data : null,
    error: enabled ? state.error : undefined,
    isLoading: enabled && state.status === "fetching",
    isError: enabled && state.status === "error",
    isSuccess: enabled && state.status === "success",
    refetch,
  }
}
