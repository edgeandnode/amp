import { createFileRoute } from "@tanstack/react-router"

import { QueryPlaygroundWrapper } from "@/Components/QueryPlayground/QueryPlaygroundWrapper"
import { osQueryOptions } from "@/hooks/useOSQuery"
import { sourcesQueryOptions } from "@/hooks/useSourcesQuery"
import { udfQueryOptions } from "@/hooks/useUDFQuery"

export const Route = createFileRoute("/")({
  component: HomePage,
  async loader({ context }) {
    await context.queryClient.ensureQueryData(osQueryOptions)
    await context.queryClient.ensureQueryData(udfQueryOptions)
    await context.queryClient.ensureQueryData(sourcesQueryOptions)
  },
})

function HomePage() {
  return <QueryPlaygroundWrapper />
}
