import {
  ArrowField,
  ArrowSchema,
  DatasetDerived,
  DatasetMetadata,
  DatasetName,
  DatasetNamespace,
  Network,
  Table,
  TableInput,
  TableSchema,
} from "@edgeandnode/amp/Model"
import { renderHook, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import { AmpConfigStreamManager, useAmpConfigStreamQuery } from "../../src/hooks/useAmpConfigStream.js"

// Mock the API_ORIGIN constant
vi.mock("../../src/constants.js", () => ({
  API_ORIGIN: "http://test-api.com",
}))

// Mock EventSource
class MockEventSource {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSED = 2
  static instances: Array<MockEventSource> = []

  url: string | URL
  listeners: Record<string, Array<(event: any) => void>> = {}
  readyState = 0
  onopen: ((event: any) => void) | null = null
  onmessage: ((event: any) => void) | null = null
  onerror: ((event: any) => void) | null = null

  constructor(url: string | URL) {
    this.url = url
    this.readyState = MockEventSource.CONNECTING
    MockEventSource.instances.push(this)

    // Simulate connection opening
    setTimeout(() => {
      this.readyState = MockEventSource.OPEN
      this.onopen?.({ type: "open" })
    }, 0)
  }

  addEventListener(type: string, listener: (event: any) => void) {
    this.listeners[type] = this.listeners[type] || []
    this.listeners[type].push(listener)
  }

  removeEventListener(type: string, listener: (event: any) => void) {
    const typeListeners = this.listeners[type]

    if (typeListeners) {
      this.listeners[type] = typeListeners.filter((l) => l !== listener)
    }
  }

  dispatchEvent(event: { type: string; data?: string }) {
    const listeners = this.listeners[event.type] || []
    listeners.forEach((listener) => listener(event))
  }

  close() {
    this.readyState = MockEventSource.CLOSED
  }

  // Test helpers
  simulateMessage(data: string) {
    this.dispatchEvent({ type: "message", data })
  }

  simulateError() {
    this.dispatchEvent({ type: "error" })
  }

  static reset() {
    MockEventSource.instances = []
  }
}

// Stub global EventSource with our mock
vi.stubGlobal("EventSource", MockEventSource)

describe("useAmpConfigStreamQuery", () => {
  beforeEach(() => {
    // Reset singleton state between tests
    AmpConfigStreamManager.reset()
    MockEventSource.reset()
    vi.clearAllMocks()
  })

  // Helper to get the most recent EventSource instance
  const getMockEventSource = () => MockEventSource.instances[MockEventSource.instances.length - 1]

  // Helper to create mock data in the format the server sends
  const createMockBuildResult = (manifest: DatasetDerived) => ({
    metadata: DatasetMetadata.make({
      namespace: DatasetNamespace.make("_"),
      name: DatasetName.make("test"),
    }),
    manifest,
  })

  it("should connect to EventSource and decode server-sent events successfully", async () => {
    // Mock SSE data in the format the API would send
    const mockManifest = DatasetDerived.make({
      kind: "manifest",
      dependencies: {},
      tables: {
        blocks: Table.make({
          network: Network.make("mainnet"),
          input: TableInput.make({ sql: "SELECT * FROM source.blocks" }),
          schema: TableSchema.make({
            arrow: ArrowSchema.make({
              fields: [
                ArrowField.make({ name: "number", type: "UInt64", nullable: false }),
                ArrowField.make({ name: "hash", type: "Utf8", nullable: false }),
              ],
            }),
          }),
        }),
      },
      functions: {},
    })

    const mockData = createMockBuildResult(mockManifest)

    const { result } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for the hook to initialize and start connecting
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate receiving SSE data
    getMockEventSource().simulateMessage(JSON.stringify(mockData))

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
    })

    expect(result.current.data).toEqual(mockData)
    expect(MockEventSource.instances.length).toBe(1)
    expect(MockEventSource.instances[0].url).toBe("http://test-api.com/config/stream")
  })

  it("should handle multiple SSE messages", async () => {
    const mockManifest1 = DatasetDerived.make({
      kind: "manifest",
      dependencies: {},
      tables: {
        blocks: Table.make({
          network: Network.make("mainnet"),
          input: TableInput.make({ sql: "SELECT * FROM source.blocks" }),
          schema: TableSchema.make({
            arrow: ArrowSchema.make({
              fields: [ArrowField.make({ name: "number", type: "UInt64", nullable: false })],
            }),
          }),
        }),
      },
      functions: {},
    })

    const mockManifest2 = DatasetDerived.make({
      kind: "manifest",
      dependencies: {},
      tables: {
        blocks: Table.make({
          network: Network.make("mainnet"),
          input: TableInput.make({ sql: "SELECT * FROM source.blocks" }),
          schema: TableSchema.make({
            arrow: ArrowSchema.make({
              fields: [
                ArrowField.make({ name: "number", type: "UInt64", nullable: false }),
                ArrowField.make({ name: "hash", type: "Utf8", nullable: false }),
              ],
            }),
          }),
        }),
      },
      functions: {},
    })

    const mockData1 = createMockBuildResult(mockManifest1)
    const mockData2 = createMockBuildResult(mockManifest2)

    const { result } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate first message
    getMockEventSource().simulateMessage(JSON.stringify(mockData1))

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
      expect(result.current.data).toEqual(mockData1)
    })

    // Simulate second message (should replace the first one)
    getMockEventSource().simulateMessage(JSON.stringify(mockData2))

    await waitFor(() => {
      expect(result.current.data).toEqual(mockData2)
    })
  })

  it("should handle EventSource errors", async () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate EventSource error
    getMockEventSource().simulateError()

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain("SSE connection error")
  })

  it("should handle malformed SSE data gracefully", async () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate malformed JSON
    getMockEventSource().simulateMessage("{invalid json}")

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain("parseJson")
  })

  it("should handle disabled state", () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery({ enabled: false }))

    // Should not connect when disabled
    expect(result.current.isLoading).toBe(false)
    expect(result.current.isError).toBe(false)
    expect(result.current.isSuccess).toBe(false)
    expect(result.current.data).toEqual(null)
    expect(MockEventSource.instances.length).toBe(0)
  })

  it("should handle refetch functionality", async () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for initial connection
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate error
    getMockEventSource().simulateError()

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    // Record instance count before refetch
    const instanceCountBefore = MockEventSource.instances.length

    // Refetch
    result.current.refetch()

    await waitFor(() => {
      expect(MockEventSource.instances.length).toBeGreaterThan(instanceCountBefore)
    })
  })

  it("should handle retry functionality", () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery({ retry: true, retryDelay: 100 }))

    // Hook should start loading immediately when enabled
    expect(result.current.isLoading).toBe(true)
    expect(result.current.isError).toBe(false)
    expect(result.current.isSuccess).toBe(false)
    expect(result.current.data).toEqual(null)
  })

  it("should handle callbacks configuration", () => {
    const onSuccess = vi.fn()
    const onError = vi.fn()
    const { result } = renderHook(() => useAmpConfigStreamQuery({ onSuccess, onError }))

    // Should accept callback configurations and start loading
    expect(result.current.isLoading).toBe(true)
    expect(result.current.data).toEqual(null)
    expect(typeof result.current.refetch).toBe("function")
  })

  it("should expose correct interface", () => {
    const { result } = renderHook(() => useAmpConfigStreamQuery({ enabled: false }))

    // Should expose the correct interface
    expect(result.current).toHaveProperty("data")
    expect(result.current).toHaveProperty("error")
    expect(result.current).toHaveProperty("isLoading")
    expect(result.current).toHaveProperty("isError")
    expect(result.current).toHaveProperty("isSuccess")
    expect(result.current).toHaveProperty("refetch")
    expect(typeof result.current.refetch).toBe("function")
  })

  it("should share single SSE connection across multiple hook instances", async () => {
    // Render the hook twice
    const { result: result1 } = renderHook(() => useAmpConfigStreamQuery())
    const { result: result2 } = renderHook(() => useAmpConfigStreamQuery())

    // Wait for both to be loading
    await waitFor(() => {
      expect(result1.current.isLoading).toBe(true)
      expect(result2.current.isLoading).toBe(true)
    })

    // EventSource should only be created ONCE for both instances
    expect(MockEventSource.instances.length).toBe(1)
    expect(MockEventSource.instances[0].url).toBe("http://test-api.com/config/stream")

    // Simulate receiving data
    const mockManifest = DatasetDerived.make({
      kind: "manifest",
      dependencies: {},
      tables: {
        blocks: Table.make({
          network: Network.make("mainnet"),
          input: TableInput.make({ sql: "SELECT * FROM source.blocks" }),
          schema: TableSchema.make({
            arrow: ArrowSchema.make({
              fields: [ArrowField.make({ name: "number", type: "UInt64", nullable: false })],
            }),
          }),
        }),
      },
      functions: {},
    })

    const mockData = createMockBuildResult(mockManifest)

    getMockEventSource().simulateMessage(JSON.stringify(mockData))

    // Both hooks should receive the same data
    await waitFor(() => {
      expect(result1.current.isSuccess).toBe(true)
      expect(result2.current.isSuccess).toBe(true)
    })

    expect(result1.current.data).toEqual(mockData)
    expect(result2.current.data).toEqual(mockData)
  })
})
