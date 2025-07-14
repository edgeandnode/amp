import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderHook, waitFor } from "@testing-library/react"
import * as Model from "nozzl/Model"
import type { ReactNode } from "react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import { useQueryableEvents } from "../../src/hooks/useQueryableEvents.js"

// Mock the API_ORIGIN constant
vi.mock("../../src/constants.js", () => ({
  API_ORIGIN: "http://test-api.com",
}))

describe("useQueryableEvents", () => {
  const createWrapper = () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    })

    return ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    )
  }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  // Helper to create a mock ReadableStream
  const createMockStream = (chunks: Array<string>) => {
    let index = 0
    return new ReadableStream({
      start(controller) {
        function push() {
          if (index < chunks.length) {
            controller.enqueue(new TextEncoder().encode(chunks[index]))
            index++
            setTimeout(push, 10) // Simulate async streaming
          } else {
            controller.close()
          }
        }
        push()
      },
    })
  }

  it("should fetch and decode server-sent events successfully", async () => {
    // Mock SSE data in the format the API would send
    const mockEventData = Model.QueryableEventStream.make({
      events: [
        Model.QueryableEvent.make({
          name: "Count",
          params: [{ name: "count", datatype: "uint256", indexed: false }],
          signature: "Count(uint256 count)",
          source: "./contracts/src/Counter.sol",
        }),
        Model.QueryableEvent.make({
          name: "Transfer",
          params: [
            { name: "from", datatype: "address", indexed: true },
            { name: "to", datatype: "address", indexed: true },
            { name: "value", datatype: "uint256", indexed: false },
          ],
          signature: "Transfer(address indexed from, address indexed to, uint256 value)",
          source: "./contracts/src/Counter.sol",
        }),
      ],
    })

    // Format as SSE
    const sseChunks = [`data: ${JSON.stringify(mockEventData)}\n\n`]

    // Mock fetch
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      body: createMockStream(sseChunks),
    } as Response)

    const { result } = renderHook(() => useQueryableEvents(), {
      wrapper: createWrapper(),
    })

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
    })

    expect(result.current.data).toEqual(mockEventData.events)
    expect(fetch).toHaveBeenCalledWith("http://test-api.com/events/stream")
  })

  it("should handle multiple SSE chunks", async () => {
    const mockEventData1 = Model.QueryableEventStream.make({
      events: [
        Model.QueryableEvent.make({
          name: "Count",
          params: [{ name: "count", datatype: "uint256", indexed: false }],
          signature: "Count(uint256 count)",
          source: "./contracts/src/Counter.sol",
        }),
      ],
    })

    const mockEventData2 = {
      events: [
        Model.QueryableEvent.make({
          name: "Transfer",
          params: [
            { name: "from", datatype: "address", indexed: true },
            { name: "to", datatype: "address", indexed: true },
            { name: "value", datatype: "uint256", indexed: false },
          ],
          signature: "Transfer(address indexed from, address indexed to, uint256 value)",
          source: "./contracts/src/Counter.sol",
        }),
      ],
    }

    // Split into multiple chunks to test streaming
    const sseChunks = [
      `data: ${JSON.stringify(mockEventData1)}\n`,
      `\ndata: ${JSON.stringify(mockEventData2)}\n\n`,
    ]

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      body: createMockStream(sseChunks),
    } as Response)

    const { result } = renderHook(() => useQueryableEvents(), {
      wrapper: createWrapper(),
    })

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
    })

    expect(result.current.data).toEqual([
      ...mockEventData1.events,
      ...mockEventData2.events,
    ])
  })

  it("should handle fetch errors", async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      statusText: "Internal Server Error",
    } as Response)

    const { result } = renderHook(() => useQueryableEvents(), {
      wrapper: createWrapper(),
    })

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain(
      "Failed to fetch events: Internal Server Error",
    )
  })

  it("should handle missing response body", async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      body: null,
    } as Response)

    const { result } = renderHook(() => useQueryableEvents(), {
      wrapper: createWrapper(),
    })

    await waitFor(() => {
      expect(result.current.isError).toBe(true)
    })

    expect(result.current.error?.message).toContain(
      "No response body available",
    )
  })

  it("should handle malformed SSE data gracefully", async () => {
    const sseChunks = [
      `data: {invalid json}\n\n`,
      `data: {"events": [{"name": "Valid", "signature": "Valid(uint256 test)", "source": "./contracts/src/Counter.sol", "params": [{"name": "test", "datatype": "uint256", "indexed": false}]}]}\n\n`,
    ]

    // Mock console.warn to verify it's called
    const consoleWarnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation(() => {})

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      body: createMockStream(sseChunks),
    } as Response)

    const { result } = renderHook(() => useQueryableEvents(), {
      wrapper: createWrapper(),
    })

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true)
    })

    // Should only have the valid event
    expect(result.current.data).toEqual([
      Model.QueryableEvent.make({
        name: "Valid",
        signature: "Valid(uint256 test)",
        source: "./contracts/src/Counter.sol",
        params: [{ name: "test", datatype: "uint256", indexed: false }],
      }),
    ])

    // Should have warned about the invalid data
    expect(consoleWarnSpy).toHaveBeenCalledWith(
      "Failed parsing QueryableEvent data from stream",
      expect.any(Object),
    )

    consoleWarnSpy.mockRestore()
  })
})
