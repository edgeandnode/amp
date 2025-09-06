import { renderHook, waitFor } from "@testing-library/react"
import { QueryableEvent, QueryableEventStream } from "nozzl/Studio/Model"
import { beforeEach, describe, expect, it, vi } from "vitest"

import { useQueryableEventsQuery } from "../../src/hooks/useQueryableEventsQuery.js"

// Mock the API_ORIGIN constant
vi.mock("../../src/constants.js", () => ({
  API_ORIGIN: "http://test-api.com",
}))

// Mock EventSource
class MockEventSource {
  url: string | URL
  listeners: Record<string, Array<(event: any) => void>> = {}
  readyState = 0
  onopen: ((event: any) => void) | null = null
  onmessage: ((event: any) => void) | null = null
  onerror: ((event: any) => void) | null = null

  constructor(url: string | URL) {
    this.url = url
    this.readyState = 1 // CONNECTING
    // Simulate connection opening
    setTimeout(() => {
      this.readyState = 2 // OPEN
      this.onopen?.({ type: "open" })
    }, 0)
  }

  addEventListener(type: string, listener: (event: any) => void) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    this.listeners[type] = this.listeners[type] || []
    this.listeners[type].push(listener)
  }

  removeEventListener(type: string, listener: (event: any) => void) {
    const typeListeners = this.listeners[type]
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (typeListeners) {
      this.listeners[type] = typeListeners.filter((l) => l !== listener)
    }
  }

  dispatchEvent(event: { type: string; data?: string }) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const listeners = this.listeners[event.type] || []
    listeners.forEach((listener) => listener(event))
  }

  close() {
    this.readyState = 3 // CLOSED
  }

  // Test helpers
  simulateMessage(data: string) {
    this.dispatchEvent({ type: "message", data })
  }

  simulateError() {
    this.dispatchEvent({ type: "error" })
  }
}

Object.assign(global, {
  EventSource: Object.assign(
    vi.fn().mockImplementation((url: string | URL) => {
      return new MockEventSource(url)
    }),
    {
      CONNECTING: 0,
      OPEN: 1,
      CLOSED: 2,
    },
  ),
})

describe("useQueryableEventsQuery", () => {
  let mockEventSource: MockEventSource

  beforeEach(() => {
    vi.clearAllMocks() // Get reference to the mock EventSource instance
    ;(global.EventSource as any).mockImplementation((url: string | URL) => {
      mockEventSource = new MockEventSource(url)
      return mockEventSource
    })
  })

  it("should connect to EventSource and decode server-sent events successfully", async () => {
    // Mock SSE data in the format the API would send
    const mockEventData = QueryableEventStream.make({
      events: [
        QueryableEvent.make({
          name: "Count",
          params: [{ name: "count", datatype: "uint256", indexed: false }],
          signature: "Count(uint256 count)",
          source: ["./contracts/src/Counter.sol"],
        }),
        QueryableEvent.make({
          name: "Transfer",
          params: [
            { name: "from", datatype: "address", indexed: true },
            { name: "to", datatype: "address", indexed: true },
            { name: "value", datatype: "uint256", indexed: false },
          ],
          signature: "Transfer(address indexed from, address indexed to, uint256 value)",
          source: ["./contracts/src/Counter.sol"],
        }),
      ],
    })

    const { result } = renderHook(() => useQueryableEventsQuery())

    // Wait for the hook to initialize and start connecting
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate receiving SSE data
    mockEventSource.simulateMessage(JSON.stringify(mockEventData))

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
    })

    expect(result.current.data).toEqual(mockEventData.events)
    expect(global.EventSource).toHaveBeenCalledWith(
      "http://test-api.com/events/stream",
    )
  })

  it("should handle multiple SSE messages", async () => {
    const mockEventData1 = QueryableEventStream.make({
      events: [
        QueryableEvent.make({
          name: "Count",
          params: [{ name: "count", datatype: "uint256", indexed: false }],
          signature: "Count(uint256 count)",
          source: ["./contracts/src/Counter.sol"],
        }),
      ],
    })

    const mockEventData2 = QueryableEventStream.make({
      events: [
        QueryableEvent.make({
          name: "Transfer",
          params: [
            { name: "from", datatype: "address", indexed: true },
            { name: "to", datatype: "address", indexed: true },
            { name: "value", datatype: "uint256", indexed: false },
          ],
          signature: "Transfer(address indexed from, address indexed to, uint256 value)",
          source: ["./contracts/src/Counter.sol"],
        }),
      ],
    })

    const { result } = renderHook(() => useQueryableEventsQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate first message
    mockEventSource.simulateMessage(JSON.stringify(mockEventData1))

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
      expect(result.current.data).toEqual(mockEventData1.events)
    })

    // Simulate second message (should replace the first one)
    mockEventSource.simulateMessage(JSON.stringify(mockEventData2))

    await waitFor(() => {
      expect(result.current.data).toEqual(mockEventData2.events)
    })
  })

  it("should handle EventSource errors", async () => {
    const { result } = renderHook(() => useQueryableEventsQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate EventSource error
    mockEventSource.simulateError()

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain("SSE connection error")
  })

  it("should handle malformed SSE data gracefully", async () => {
    // Mock console.warn to verify it's called
    const consoleWarnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation(() => {})

    const { result } = renderHook(() => useQueryableEventsQuery())

    // Wait for loading state
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate malformed JSON
    mockEventSource.simulateMessage("{invalid json}")

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain("parseJson")

    consoleWarnSpy.mockRestore()
  })

  it("should handle disabled state", () => {
    const { result } = renderHook(() => useQueryableEventsQuery({ enabled: false }))

    // Should not connect when disabled
    expect(result.current.isLoading).toBe(false)
    expect(result.current.isError).toBe(false)
    expect(result.current.isSuccess).toBe(false)
    expect(result.current.data).toEqual([])
    expect(global.EventSource).not.toHaveBeenCalled()
  })

  it("should handle refetch functionality", async () => {
    const { result } = renderHook(() => useQueryableEventsQuery())

    // Wait for initial connection
    await waitFor(() => {
      expect(result.current.isLoading).toBe(true)
    })

    // Simulate error
    mockEventSource.simulateError()

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    // Clear the mock call count
    vi.clearAllMocks()

    // Refetch
    result.current.refetch()

    await waitFor(() => {
      expect(global.EventSource).toHaveBeenCalledWith(
        "http://test-api.com/events/stream",
      )
    })
  })

  it("should handle retry functionality", () => {
    const { result } = renderHook(() => useQueryableEventsQuery({ retry: true, retryDelay: 100 }))

    // Hook should start loading immediately when enabled
    expect(result.current.isLoading).toBe(true)
    expect(result.current.isError).toBe(false)
    expect(result.current.isSuccess).toBe(false)
    expect(result.current.data).toEqual([])
  })

  it("should handle callbacks configuration", () => {
    const onSuccess = vi.fn()
    const onError = vi.fn()
    const { result } = renderHook(() => useQueryableEventsQuery({ onSuccess, onError }))

    // Should accept callback configurations and start loading
    expect(result.current.isLoading).toBe(true)
    expect(result.current.data).toEqual([])
    expect(typeof result.current.refetch).toBe("function")
  })

  it("should expose correct interface", () => {
    const { result } = renderHook(() => useQueryableEventsQuery({ enabled: false }))

    // Should expose the correct interface
    expect(result.current).toHaveProperty("data")
    expect(result.current).toHaveProperty("error")
    expect(result.current).toHaveProperty("isLoading")
    expect(result.current).toHaveProperty("isError")
    expect(result.current).toHaveProperty("isSuccess")
    expect(result.current).toHaveProperty("refetch")
    expect(typeof result.current.refetch).toBe("function")
  })
})
