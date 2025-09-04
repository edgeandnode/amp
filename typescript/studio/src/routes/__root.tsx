import type { QueryClient } from "@tanstack/react-query"
import { createRootRouteWithContext, Outlet } from "@tanstack/react-router"
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools"

export interface DatasetWorksRouterCtx {
  readonly queryClient: QueryClient
}

export const Route = createRootRouteWithContext<DatasetWorksRouterCtx>()({
  component() {
    return (
      <div className="flex flex-col h-full min-h-screen w-screen">
        <Outlet />
        <TanStackRouterDevtools />
      </div>
    )
  },
})
