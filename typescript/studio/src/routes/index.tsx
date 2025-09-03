import { createFileRoute } from "@tanstack/react-router"

import { MetadataBrowser } from "../Components/QueryPlayground/MetadataBrowser"
import { QueryPlaygroundWrapper } from "../Components/QueryPlayground/QueryPlaygroundWrapper"
import { SchemaBrowser } from "../Components/QueryPlayground/SchemaBrowser"
import { UDFBrowser } from "../Components/QueryPlayground/UDFBrowser"
import { metadataQueryOptions } from "../hooks/useMetadataQuery"
import { osQueryOptions } from "../hooks/useOSQuery"
import { udfQueryOptions } from "../hooks/useUDFQuery"

export const Route = createFileRoute("/")({
  component: HomePage,
  async loader({ context }) {
    await context.queryClient.ensureQueryData(osQueryOptions)
    await context.queryClient.ensureQueryData(udfQueryOptions)
    await context.queryClient.ensureQueryData(metadataQueryOptions)
  },
})

function HomePage() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 xl:grid-cols-4 h-full min-h-screen">
      <div className="md:col-span-2 xl:col-span-3">
        <QueryPlaygroundWrapper />
      </div>
      <div className="h-full border-l border-space-1500 flex flex-col gap-y-4 overflow-y-auto">
        <SchemaBrowser />
        <MetadataBrowser />
        <UDFBrowser />
      </div>
    </div>
  )
}
