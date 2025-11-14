"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import type { StudioModel } from "@edgeandnode/amp"
import { Button, Divider } from "@graphprotocol/gds-react"
import { FolderIcon, FolderOpenIcon, PlusIcon } from "@graphprotocol/gds-react/icons"

import { useSourcesSuspenseQuery } from "@/hooks/useSourcesQuery"

export type SourcesBrowserProps = {
  onSourceSelected: (source: StudioModel.DatasetSource) => void
}
export function SourcesBrowser({ onSourceSelected: onTableSelected }: Readonly<SourcesBrowserProps>) {
  const { data: sources } = useSourcesSuspenseQuery()

  return (
    <div className="flex flex-col gap-4 px-2 py-6">
      <div className="flex flex-col gap-1 px-4">
        <p className="text-14">Sources</p>
        <p className="text-12 text-fg-muted">Root dataset source tables that can be queried.</p>
      </div>
      <Accordion.Root className="flex flex-col gap-0.5">
        {sources.map((source) => (
          <Accordion.Item key={source.source} className="flex flex-col gap-2 data-open:bg-bg-muted rounded-8">
            <Accordion.Header className="flex gap-1 px-4 py-2 w-full hover:bg-bg-default rounded-6 justify-between items-center">
              <Accordion.Trigger type="button" className="flex group flex-1 gap-1">
                <FolderIcon className="group-data-panel-open:hidden block" aria-hidden="true" size={5} alt="" />
                <FolderOpenIcon className="group-data-panel-open:block hidden" aria-hidden="true" size={5} alt="" />
                <span className="text-14">{source.source}</span>
              </Accordion.Trigger>
              <Button variant="naked" size="large" onClick={() => onTableSelected(source)}>
                <PlusIcon alt={`Add ${source.source}`} size={4} aria-hidden="true" />
              </Button>
            </Accordion.Header>
            <Accordion.Panel className="mb-4 flex flex-col px-4">
              {source.metadata_columns.map((column) => (
                <div
                  key={`${source.source}__${column.name}`}
                  className="border-border-muted ml-2 flex items-center border-l py-1 pb-2"
                >
                  <Divider className="mr-2 w-2" />
                  <div className="flex w-full items-center justify-between">
                    <span className="text-14">{column.name}</span>
                    <span className="ml-auto text-brand-200">{column.datatype}</span>
                  </div>
                </div>
              ))}
            </Accordion.Panel>
          </Accordion.Item>
        ))}
      </Accordion.Root>
    </div>
  )
}
