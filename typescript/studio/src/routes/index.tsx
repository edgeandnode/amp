"use client"

import { AmpLogoIcon } from "@graphprotocol/gds-react/icons"
import { createFileRoute } from "@tanstack/react-router"

import { QueryPlaygroundWrapper } from "@/Components/QueryPlayground/QueryPlaygroundWrapper"
import { useAmpConfigStreamQuery } from "@/hooks/useAmpConfigStream"
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
  const { data } = useAmpConfigStreamQuery()

  return (
    <div className="w-full h-full min-h-screen border-collapse">
      <div className="w-20 fixed inset-y-0 z-50 flex flex-col items-center justify-start h-full min-h-screen border-r border-space-1500">
        <div className="inline-flex items-center justify-center w-full h-16">
          <AmpLogoIcon variant="branded" size={8} alt="" aria-hidden="true" />
        </div>
      </div>

      <div className="pl-20 sticky top-0 z-40 flex items-center h-16 bg-space-1700">
        <div className="flex items-center gap-x-2 px-4 h-16 text-space-500 w-full border-b border-space-1500">
          <p className="text-16 text-space-500">Local Studio</p>
          {data != null ?
            (
              <>
                <svg
                  fill="currentColor"
                  viewBox="0 0 20 20"
                  aria-hidden="true"
                  className="size-5 shrink-0 text-gray-300 dark:text-gray-600"
                >
                  <path d="M5.555 17.776l8-16 .894.448-8 16-.894-.448z" />
                </svg>
                <p className="text-white text-16">{data.name}</p>
              </>
            ) :
            null}
        </div>
      </div>

      <main className="pl-20">
        <QueryPlaygroundWrapper />
      </main>
    </div>
  )
}
