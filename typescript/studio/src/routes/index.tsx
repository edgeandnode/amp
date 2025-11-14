"use client"

import { AmpLogoIcon } from "@graphprotocol/gds-react/icons"
import { createFileRoute } from "@tanstack/react-router"

import { QueryPlaygroundWrapper } from "@/Components/QueryPlayground/QueryPlaygroundWrapper"
import { useAmpConfigStreamQuery } from "@/hooks/useAmpConfigStream"
import { defaultQueryOptions } from "@/hooks/useDefaultQuery"
import { osQueryOptions } from "@/hooks/useOSQuery"
import { sourcesQueryOptions } from "@/hooks/useSourcesQuery"
import { Breadcrumbs } from "@graphprotocol/gds-react"

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
    <div className="flex min-h-dvh w-full">
      <div className="w-14 sticky inset-y-0 px-2 py-3 h-dvh flex flex-col items-center border-r bg-bg-subtle border-border-muted">
        <AmpLogoIcon variant="branded" size={8} alt="" aria-hidden="true" />
      </div>

      <main className="flex flex-col w-full">
        <div className="sticky top-0 z-40 flex items-center h-16 bg-bg-subtle border-b border-border-muted px-4">
          <Breadcrumbs>
            <Breadcrumbs.Item>Local Studio</Breadcrumbs.Item>
            {data != null ?
              (
                <Breadcrumbs.Item>
                  {/* TODO: DatasetManifest no longer has 'name' field. Using backwards-compatible fallback until metadata is passed separately. */}
                  <p className="text-white text-16">{("name" in data ? data.name : "unknown") as string}</p>
                </Breadcrumbs.Item>
              ) :
              null}
          </Breadcrumbs>
        </div>

        <QueryPlaygroundWrapper />
      </main>
    </div>
  )
}
