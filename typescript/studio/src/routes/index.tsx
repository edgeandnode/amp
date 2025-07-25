import { createFileRoute } from "@tanstack/react-router"

import { SchemaBrowser } from "@/Components/QueryPlayground/SchemaBrowser"

import { QueryPlaygroundWrapper } from "../Components/QueryPlayground/QueryPlaygroundWrapper"
import { UDF } from "../Components/QueryPlayground/UDF"
import { udfQueryOptions } from "../Components/QueryPlayground/useUDFQuery"
import { osQueryOptions } from "../hooks/useOSQuery"

export const Route = createFileRoute("/")({
  component: HomePage,
  async loader({ context }) {
    await context.queryClient.ensureQueryData(osQueryOptions)
    await context.queryClient.ensureQueryData(udfQueryOptions)
  },
})

function HomePage() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 xl:grid-cols-4 h-full min-h-screen">
      <div className="md:col-span-2 xl:col-span-3">
        <QueryPlaygroundWrapper />
      </div>
      <div className="h-full border-l border-white/10 flex flex-col gap-y-4 overflow-y-auto">
        <SchemaBrowser />
        <UDF />
      </div>
    </div>
  )
}
