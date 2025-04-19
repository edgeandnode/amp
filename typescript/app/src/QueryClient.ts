import { defaultShouldDehydrateQuery, QueryClient } from "@tanstack/react-query"

export const queryClient = new QueryClient({
  defaultOptions: {
    dehydrate: {
      shouldDehydrateQuery(query) {
        return defaultShouldDehydrateQuery(query) || query.state.status === "pending"
      }
    }
  }
})
