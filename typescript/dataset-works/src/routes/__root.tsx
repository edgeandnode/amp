import type { QueryClient } from "@tanstack/react-query"
import { createRootRouteWithContext, Outlet } from "@tanstack/react-router"
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools"

export interface DatasetWorksRouterCtx {
  readonly queryClient: QueryClient
}

export const Route = createRootRouteWithContext<DatasetWorksRouterCtx>()({
  component() {
    return (
      <>
        <Outlet />
        <TanStackRouterDevtools />
      </>
    )
  },
})
