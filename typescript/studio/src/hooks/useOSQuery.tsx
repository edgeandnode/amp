"use client"

import type { UseSuspenseQueryOptions } from "@tanstack/react-query"
import { queryOptions, useSuspenseQuery } from "@tanstack/react-query"

// Type declaration for the experimental userAgentData API
interface NavigatorUAData {
  platform: string
}

declare global {
  interface Navigator {
    userAgentData?: NavigatorUAData
  }
}

type OS = "Windows" | "MacOS" | "Linux" | "unknown"

export const osQueryOptions = queryOptions<OS, Error, OS, readonly ["OS"]>({
  queryKey: ["OS"] as const,
  async queryFn() {
    const userAgent = window.navigator.userAgent

    // Try modern API first (navigator.userAgentData)
    if ("userAgentData" in navigator && navigator.userAgentData.platform) {
      const platform = navigator.userAgentData.platform.toLowerCase()

      if (platform.includes("windows")) {
        return await Promise.resolve("Windows" as const)
      }

      if (platform.includes("macos") || platform.includes("mac")) {
        return await Promise.resolve("MacOS" as const)
      }

      if (platform.includes("linux")) {
        return await Promise.resolve("Linux" as const)
      }
    }

    // Fallback to userAgent string parsing
    if (userAgent.indexOf("Win") !== -1) {
      return await Promise.resolve("Windows" as const)
    }

    if (userAgent.indexOf("Mac") !== -1) {
      return await Promise.resolve("MacOS" as const)
    }

    if (userAgent.indexOf("Linux") !== -1) {
      return await Promise.resolve("Linux" as const)
    }

    return await Promise.resolve("unknown" as const)
  },
})

export function useOSQuery(
  options: Omit<UseSuspenseQueryOptions<OS, Error, OS, readonly ["OS"]>, "queryKey" | "queryFn"> = {},
) {
  return useSuspenseQuery({
    ...osQueryOptions,
    ...options,
    staleTime: Number.POSITIVE_INFINITY,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  })
}
