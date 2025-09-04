import { createFileRoute } from "@tanstack/react-router"

import { QueryPlaygroundWrapper } from "@/Components/QueryPlayground/QueryPlaygroundWrapper"
import { metadataQueryOptions } from "@/hooks/useMetadataQuery"
import { osQueryOptions } from "@/hooks/useOSQuery"
import { udfQueryOptions } from "@/hooks/useUDFQuery"

export const Route = createFileRoute("/")({
  component: HomePage,
  async loader({ context }) {
    await context.queryClient.ensureQueryData(osQueryOptions)
    await context.queryClient.ensureQueryData(udfQueryOptions)
    await context.queryClient.ensureQueryData(metadataQueryOptions)
  },
})

function HomePage() {
  return <QueryPlaygroundWrapper />
}
