import { QueryClientProvider } from "@tanstack/react-query"

import { queryClient } from "./QueryClient"

export default function Providers({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  )
}
