"use client"

import { Accordion } from "@base-ui-components/react/accordion"
import { Model } from "@edgeandnode/amp"
import { Button } from "@graphprotocol/gds-react"
import { PlusIcon, TableIcon } from "@graphprotocol/gds-react/icons"
import { String } from "effect"

import { useAmpConfigStreamQuery } from "@/hooks/useAmpConfigStream"

export type AmpConfigBrowserProps = {
  onTableSelected: (dataset: Model.DatasetReferenceString, table: string) => void
}
export function AmpConfigBrowser({ onTableSelected }: Readonly<AmpConfigBrowserProps>) {
  const { data: config } = useAmpConfigStreamQuery()

  if (config == null) {
    return null
  }

  const tables = Object.entries(config.manifest.tables)

  return (
    <div className="flex flex-col gap-4 px-2 py-6">
      <div className="flex flex-col gap-1 px-4">
        <p className="text-14">Dataset Tables</p>
        <p className="text-12 text-fg-muted">Tables derived from your current config.</p>
      </div>
      {tables.length > 0 ?
        (
          <Accordion.Root className="flex flex-col gap-0.5">
            {tables.map(([table, def]) => (
              <Accordion.Item key={table} className="flex flex-col gap-2 data-open:bg-bg-muted rounded-8">
                <Accordion.Header className="flex gap-1 px-4 py-2 w-full hover:bg-bg-default rounded-6 justify-between items-center">
                  <Accordion.Trigger type="button" className="flex flex-1 gap-1">
                    <TableIcon className="text-solar-500" aria-hidden="true" size={5} alt="" />
                    <span className="text-14">{table}</span>
                  </Accordion.Trigger>
                  <Button
                    variant="naked"
                    size="large"
                    onClick={() => {
                      const namespace = Model.DatasetNamespace.make(config.metadata.namespace || "_")
                      const name = Model.DatasetName.make(config.metadata.name)
                      const revision = Model.DatasetTag.make("dev")
                      const ref: Model.DatasetReferenceString = `${namespace}/${name}@${revision}`
                      onTableSelected(ref, table)
                    }}
                  >
                    <PlusIcon alt={`Add ${table}`} size={4} aria-hidden="true" />
                  </Button>
                </Accordion.Header>
                <Accordion.Panel className="mb-4 flex flex-col px-4">
                  <pre className="bg-bg-canvas text-fg-default p-3 rounded-8 text-14 font-mono overflow-x-auto">
                  <code>{"input" in def ? String.trim((def as any).input.sql) : "No SQL (raw dataset)"}</code>
                  </pre>
                </Accordion.Panel>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        ) :
        (
          <div className="flex flex-col items-center bg-bg-canvas border border-border-muted justify-center rounded-8 gap-4 p-6">
            <div className="p-2 rounded-8 bg-solar-1000 inline-flex items-center justify-center">
              <TableIcon size={5} alt="" aria-hidden="true" className="text-solar-200" />
            </div>
            <div className="flex flex-col items-center gap-1">
              <p className="text-14">No Tables Available</p>
              <p className="text-12 text-fg-muted text-center">
                Add tables to your amp dataset config and they will show up here.
              </p>
            </div>
          </div>
        )}
    </div>
  )
}
