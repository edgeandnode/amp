import { createFileRoute } from "@tanstack/react-router"

import { QueryPlaygroundWrapper } from "@/Components/QueryPlayground/QueryPlaygroundWrapper"
import { defaultQueryOptions } from "@/hooks/useDefaultQuery"
import { osQueryOptions } from "@/hooks/useOSQuery"
import { sourcesQueryOptions } from "@/hooks/useSourcesQuery"

export const Route = createFileRoute("/")({
  component: HomePage,
  async loader({ context }) {
    await Promise.all([
      context.queryClient.ensureQueryData(defaultQueryOptions),
      context.queryClient.ensureQueryData(osQueryOptions),
      context.queryClient.ensureQueryData(sourcesQueryOptions),
    ])
  },
})

function HomePage() {
  return <QueryPlaygroundWrapper />
}
