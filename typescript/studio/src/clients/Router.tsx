import {
  defaultShouldDehydrateQuery,
  dehydrate,
  hydrate,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query"
import { ReactQueryDevtools } from "@tanstack/react-query-devtools"
import { createRouter } from "@tanstack/react-router"
import type { ReactNode } from "react"

import type { DatasetWorksRouterCtx } from "../routes/__root"
import { routeTree } from "../routeTree.gen"

import { transport } from "./Nozzle"

export function createDatasetWorksAppRouter() {
  const queryClient = new QueryClient({
    defaultOptions: {
      dehydrate: {
        shouldDehydrateQuery(query) {
          return (
            defaultShouldDehydrateQuery(query) ||
            query.state.status === "pending"
          )
        },
      },
    },
  })

  return createRouter({
    routeTree,
    defaultPreload: "intent",
    scrollRestoration: true,
    defaultStructuralSharing: true,
    defaultPreloadStaleTime: 0,
    context: {
      queryClient,
      transport,
    } as const satisfies DatasetWorksRouterCtx,
    // On the server, dehydrate the loader client so the router
    // can serialize it and send it to the client for us
    dehydrate() {
      return {
        queryClientState: dehydrate(queryClient),
      } as const
    },
    // On the client, hydrate the loader client with the data
    // we dehydrated on the server
    hydrate(dehydrated: { queryClientState: unknown }) {
      hydrate(queryClient, dehydrated.queryClientState)
    },
    Wrap({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          {children}
          <ReactQueryDevtools />
        </QueryClientProvider>
      )
    },
  })
}

export type DatasetWorksAppRouter = ReturnType<
  typeof createDatasetWorksAppRouter
>
